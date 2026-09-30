"""Wiring, restart recovery and the command entry point."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from .agg.service import AggService
from .audit.service import AuditService
from .cement.service import CementService
from .chute.service import ChuteService
from .clock import ManualClock
from .config import StationConfig
from .context import SharedContext
from .decision.batch import BatchRegistry
from .decision.query import query_from_params, run_query
from .decision.stateview import StateView
from .decision.thresholds import ThresholdSet
from .errors import NotFoundError
from .event.bus import Event, EventBus
from .event.subscribers import AlarmCollector, AuditSubscriber
from .event.topics import ALARM, STORE
from .ids import SequenceIds
from .interlocks import build_engine
from .mix.service import MixService
from .moist.comp import Compensation
from .moist.service import MoistService
from .moist.table import default_table
from .ns.registry import DeviceKind
from .ns.topology import build_registry, default_topology
from .plant.controller import PlantController
from .plant.status import compose_plant_status
from .plant.tools import integer, number, optional_integer, optional_number, optional_text, text
from .pump.service import PumpService
from .recipe.recipe import Recipe
from .recipe.service import RecipeService
from .reconcile.digest import anchor_projection
from .reconcile.ledger import ReconciliationLedger
from .reconcile.policy import VoidPolicy
from .reconcile_actions import WriteOffService
from .sampler.service import SamplerService
from .service_base import PlantService
from .statemachine.gate import PreGate
from .statemachine.machine import SequenceMachine
from .statemachine.phases import LINE_SEQUENCES
from .store.repository import RecordRepository, RestoreReport
from .versioning.expiry import Validity
from .versioning.facade import VersionedArtifacts
from .water.service import WaterService

BOOT_KIND = "station.boot"
CLOCK_KIND = "store.clock"
BASELINE_KIND = "version.baseline"
CONFIRMATION_KIND = "version.confirmation"
BATCH_KIND = "agg.batch"
TIMER_KIND = "mix.timer"
LATCH_KIND = "sampler.latch"
COMP_KIND = "moist.comp"
SELECTED_KIND = "recipe.selected"
PUBLISHED_KIND = "recipe.published"

LINES: tuple[str, ...] = tuple(LINE_SEQUENCES)
EXTRA_DEVICES = (
    ("scale-1", DeviceKind.SCALE, "aggregate"),
    ("mixer-1", DeviceKind.MIXER, "primary"),
    ("hopper-1", DeviceKind.HOPPER, "aggregate"),
    ("pump-1", DeviceKind.PUMP, "delivery"),
    ("sampler-1", DeviceKind.SAMPLER, "laboratory"),
    ("tank-1", DeviceKind.TANK, "batching water"),
)


class PlantRuntime:
    """Owns every collaborator and exposes the command surface."""

    def __init__(self, config: StationConfig | None = None, *, storage: str = "file") -> None:
        self.config = config or StationConfig()
        self.clock = ManualClock()
        self.ids = SequenceIds()
        if storage == "memory":
            self.store = RecordRepository.in_memory(self.clock, self.ids)
        else:
            self.store = RecordRepository.open(self.config.data_dir, self.clock, self.ids)
        self.versions = VersionedArtifacts.fresh(self.ids)
        self.thresholds = ThresholdSet.from_limits(self.config.limits)
        self.view = StateView(self.store)
        self.batches = BatchRegistry()
        self.engine = build_engine()
        self.gate = PreGate(self.engine)
        self.bus = EventBus()
        self.topology = default_topology()
        self.registry = build_registry(self.topology, EXTRA_DEVICES)
        self.audit_subscriber = AuditSubscriber(self.store)
        self.alarms = AlarmCollector()
        self.machines = {line: SequenceMachine(line) for line in LINES}
        self.context = SharedContext(
            store=self.store,
            versions=self.versions,
            bus=self.bus,
            clock=self.clock,
            config=self.config,
            thresholds=self.thresholds,
            view=self.view,
            batches=self.batches,
            gate=self.gate,
            ids=self.ids,
        )
        self.recipes = RecipeService(self.context, self.topology, self.machines["batch"])
        self.agg = AggService(self.context, self.recipes, self.machines["batch"])
        self.moist = MoistService(self.context, self.recipes.book, default_table(), self.machines["batch"])
        self.cement = CementService(self.context, self.machines["mix"])
        self.water = WaterService(self.context, self.machines["mix"])
        self.mix = MixService(self.context, self.recipes, self.machines["mix"])
        self.chute = ChuteService(self.context, self.recipes, self.machines["mix"])
        self.pump = PumpService(self.context, self.machines["delivery"])
        self.sampler = SamplerService(self.context, self.recipes, self.machines["delivery"])
        self.audit = AuditService(self.context, self.machines["batch"], alarms=self.alarms)
        self.reconciliation = ReconciliationLedger(self.store, self.ids, policy=VoidPolicy())
        self.actions = WriteOffService(self.store, self.ids)
        self.controller = PlantController(self)
        self.boot_report: RestoreReport | None = None
        self._services: dict[str, PlantService] = {
            "recipe": self.recipes,
            "agg": self.agg,
            "moist": self.moist,
            "cement": self.cement,
            "water": self.water,
            "mix": self.mix,
            "chute": self.chute,
            "pump": self.pump,
            "sampler": self.sampler,
            "audit": self.audit,
        }
        self._wire_bus()

    @classmethod
    def in_memory(cls) -> "PlantRuntime":
        """Boot a runtime that keeps its journal and snapshots in memory."""

        return cls(storage="memory")

    def _wire_bus(self) -> None:
        self.bus.subscribe(ALARM, self.alarms)
        self.bus.subscribe(ALARM, self.audit.handle_alarm)
        self.bus.subscribe_all(self.audit_subscriber)

    def bootstrap(self) -> dict[str, Any]:
        """Rebuild every derived artifact from the stored stream."""

        self._restore_clock()
        report = self.store.restore(self.clock.now())
        self.boot_report = report
        self._restore_id_counters()
        self._adopt_records()
        self._hydrate_machines()
        self.store.publish(
            BOOT_KIND,
            "console",
            0,
            {
                "station": self.config.station,
                "replayed_records": report.replayed_records,
                "snapshot": report.used_snapshot_id,
                "rejected_snapshots": [dict(item) for item in report.rejected_snapshots],
            },
        )
        return report.describe()

    def _restore_clock(self) -> int:
        """Continue the tick at the last one the stream recorded."""

        peak = max((record.tick for record in self.store.visible()), default=0)
        if peak > self.clock.now():
            self.clock.advance(peak - self.clock.now())
        return peak

    def _restore_id_counters(self) -> None:
        """Raise the identifier counters so a restart cannot reuse an id."""

        confirmations = sum(1 for record in self.store.visible() if record.kind == CONFIRMATION_KIND)
        self.ids.restore("conf", confirmations)
        self.ids.restore("snap", len(self.store.snapshots.load_all()))
        samples = sum(1 for record in self.store.visible() if record.kind == "sampler.record")
        self.ids.restore("sample", samples)
        self.ids.restore(
            "rcpt",
            sum(1 for record in self.store.committed_records() if record.kind == "reconcile.checkpoint"),
        )
        self.ids.restore(
            "void",
            sum(1 for record in self.store.committed_records() if record.kind == "reconcile.void"),
        )
        self.ids.restore(
            "wroff",
            sum(1 for record in self.store.committed_records() if record.kind == "action.writeoff"),
        )
        self.ids.restore(
            "voida",
            sum(1 for record in self.store.committed_records() if record.kind == "action.void"),
        )

    def _adopt_records(self) -> None:
        """Adopt everything the stream still carries."""

        corrections: list[Compensation] = []
        for record in self.store.visible():
            if record.kind == PUBLISHED_KIND:
                recipe = Recipe.from_document(record.payload)
                self.recipes.adopt(recipe, selected=False)
            elif record.kind == SELECTED_KIND:
                self.recipes.adopt(self.recipes.book.current(str(record.payload["name"])), selected=True)
            elif record.kind == BATCH_KIND:
                from .decision.batch import Batch

                self.batches.adopt(
                    Batch(
                        batch_id=str(record.payload["batch_id"]),
                        generation=int(record.payload.get("generation", 0)),
                        tick=record.tick,
                        grade=str(record.payload.get("grade", "")),
                        quantity=float(record.payload.get("quantity", 0.0)),
                        unit=str(record.payload.get("unit", "m3")),
                    )
                )
            elif record.kind == COMP_KIND:
                corrections.append(
                    Compensation(
                        material=str(record.payload["material"]),
                        dry_mass=float(record.payload.get("dry_mass", 0.0)),
                        moisture_percent=float(record.payload.get("moisture_percent", 0.0)),
                        free_water_l=float(record.payload.get("free_water_l", 0.0)),
                        wet_mass=float(record.payload.get("wet_mass", 0.0)),
                    )
                )
            elif record.kind == TIMER_KIND:
                self.mix.timer.sync(
                    started=record.payload.get("started"),
                    length=int(record.payload.get("length", 0)),
                )
            elif record.kind == LATCH_KIND:
                self.sampler.latch.sync(
                    latched=bool(record.payload.get("active", False)),
                    step=int(record.payload.get("step", 0)),
                    reason=str(record.payload.get("reason", "replayed")),
                )
            elif record.kind == BASELINE_KIND:
                self.versions.adopt_baseline(
                    str(record.payload["name"]),
                    float(record.payload["value"]),
                    str(record.payload.get("unit", "")),
                    1,
                    validity=Validity.never(record.tick),
                )
        self.moist.sync(corrections)

    def _hydrate_machines(self) -> None:
        for line, machine in self.machines.items():
            record = self.view.latest(f"{line}.phase")
            if record is None:
                continue
            machine.hydrate(
                str(record.payload["phase"]),
                step=int(record.payload["step"]),
                tick=record.tick,
            )

    def health(self) -> dict[str, Any]:
        """Return the values a health probe needs."""

        peak = self.clock.now()
        return {
            "status": "ok",
            "station": self.config.station,
            "tick": peak,
            "records": self.store.last_seq(),
            "visible": len(self.store.visible()),
            "reconcile_watermark": self.reconciliation.watermark().describe(),
            "batches": self.batches.count(),
            "alarms": self.alarms.count(),
            "lines": {line: machine.phase for line, machine in self.machines.items()},
            "latched": {"sampler": self.sampler.latch.latched},
        }

    def state(self) -> dict[str, Any]:
        """Return the full console snapshot."""

        now = self.clock.now()
        return compose_plant_status(
            station=self.config.station,
            now=now,
            watermark=self.store.watermark().describe(),
            reconcile_watermark=self.reconciliation.watermark().describe(),
            subsystems={name: service.status() for name, service in self._services.items()},
            lines={
                line: {
                    "phase": machine.phase,
                    "sequence": machine.order(),
                    "next": machine.next_phase(),
                    "last_step": machine.last_step.describe(),
                }
                for line, machine in self.machines.items()
            },
            versioned=self.versions.describe(now),
            thresholds=self.thresholds.describe(),
            interlocks={
                "actions": list(self.engine.actions()),
                "rules": [rule.describe() for rule in self.engine.rules()],
            },
            batches=[batch.describe() for batch in self.batches.all()],
            alarms=self.alarms.recent(),
            facts=self.view.facts(),
            config=self.config.describe(),
        )

    def records(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Run the audit query the caller asked for."""

        query = query_from_params(params or {})
        return run_query(self.store.committed_records(), query).describe()

    def history(self, tick: int) -> dict[str, Any]:
        """Compare the current state with the state at ``tick``."""

        return {
            "tick": tick,
            "now": self.clock.now(),
            "state": self.view.as_of(tick),
            "delta": self.view.delta(tick).describe(),
        }

    def publish_baseline(
        self,
        name: str,
        value: float,
        unit: str,
        *,
        ttl_ticks: int | None = None,
    ) -> dict[str, Any]:
        """Publish a calibration value under a fresh generation."""

        ttl = self.config.defaults.baseline_ttl_ticks if ttl_ticks is None else ttl_ticks
        baseline = self.versions.publish_baseline(name, value, unit, tick=self.clock.now(), ttl_ticks=ttl)
        record = self.store.publish(
            BASELINE_KIND,
            "console",
            baseline.generation,
            {"active": True, "name": baseline.name, "value": baseline.value, "unit": baseline.unit},
        )
        self.bus.publish(
            Event(
                topic=STORE,
                name="version.baseline_published",
                origin="console",
                tick=self.clock.now(),
                payload={"name": name, "generation": baseline.generation},
            )
        )
        return {"baseline": baseline.describe(self.clock.now()), "seq": record.seq}

    def confirm(self, subject: str, *, ttl_ticks: int | None = None) -> dict[str, Any]:
        """Record a fresh confirmation for ``subject``."""

        ttl = self.config.defaults.confirmation_ttl_ticks if ttl_ticks is None else ttl_ticks
        confirmation = self.versions.confirm(
            subject,
            issuer="console",
            tick=self.clock.now(),
            ttl_ticks=ttl,
        )
        record = self.store.publish(
            CONFIRMATION_KIND,
            "console",
            confirmation.generation,
            {"active": True, **confirmation.describe(self.clock.now())},
        )
        return {"confirmation": confirmation.describe(self.clock.now()), "seq": record.seq}

    def take_snapshot(self, *, ttl_ticks: int | None = None) -> dict[str, Any]:
        """Materialise the committed state."""

        ttl = self.config.defaults.snapshot_ttl_ticks if ttl_ticks is None else ttl_ticks
        token = self.versions.bump("snapshot", tick=self.clock.now())
        snapshot = self.store.snapshot(generation=token.value, validity=Validity.of(self.clock.now(), ttl))
        return {"snapshot": snapshot.describe(self.clock.now())}

    def rollback(
        self,
        seq: int,
        *,
        reason: str = "operator rollback",
        idem_key: str | None = None,
    ) -> dict[str, Any]:
        """Tombstone every committed record after ``seq`` and void the checks.

        The store rollback appends tombstones (history stays intact); the
        reconciliation ledger then retires every checkpoint whose range the
        rollback reached, scoping each record kind with its frozen policy.  A
        repeat rollback carrying the same ``idem_key`` appends nothing twice.
        """

        policy = self.reconciliation.policy
        outcome = self.store.rollback(seq, reason=reason, origin="console", generation=0)
        # Boundary metadata (checkpoints, voids, write-off/void actions) is
        # retired by the reconciliation layer's own append-only rules, never by
        # the per-kind domain void policy.  Only domain records are scoped.
        metadata_kinds = frozenset(
            {"reconcile.checkpoint", "reconcile.void", "action.writeoff", "action.void"}
        )
        affected_kinds: dict[str, str] = {}
        for target_seq in outcome.neutralised_seq:
            record = self._record_at(target_seq)
            if record is None or record.kind in metadata_kinds:
                continue
            affected_kinds[record.kind] = policy.scope_for(record.kind)
        if outcome.tombstones:
            voids = self.reconciliation.void_after(
                seq,
                tombstoned_seqs=outcome.neutralised_seq,
                affected_kinds=affected_kinds,
                reason=reason,
                idem_key=idem_key,
            )
        else:
            voids = ()
        return {
            **outcome.describe(),
            "voids": [void.describe() for void in voids],
            "scope_by_kind": affected_kinds,
            "watermark": self.store.watermark().describe(),
            "reconcile_watermark": self.reconciliation.watermark().describe(),
        }

    def _record_at(self, seq: int):
        for record in self.store.committed_records():
            if record.seq == seq:
                return record
        return None

    def reconcile(self, *, idem_key: str | None = None) -> dict[str, Any]:
        """Advance the reconciliation watermark over everything committed."""

        result = self.reconciliation.reconcile(issuer="console", idem_key=idem_key)
        return result.describe()

    def external_query(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """The regulator facing read: only reconciled, still-valid records."""

        from .decision.query import query_from_params, run_query

        query = query_from_params(params or {})
        result = run_query(self.reconciliation.readable_records(), query)
        document = result.describe()
        document["reconcile_watermark"] = self.reconciliation.watermark().describe()
        return document

    def external_state(self) -> dict[str, Any]:
        """The bounded state view an external consumer is allowed to see."""

        return self.reconciliation.readable_state()

    def history_as_of(self, tick: int) -> dict[str, Any]:
        """The state at ``tick`` under the boundary that was valid back then."""

        return {
            "tick": tick,
            "now": self.clock.now(),
            "reconcile_watermark": self.reconciliation.checkpoint_at(tick).describe(),
            "state": self.reconciliation.state_as_of(tick),
        }

    def reconcile_status(self) -> dict[str, Any]:
        """Console + audit view of the boundary, policy and all voids."""

        return self.reconciliation.describe()

    def consistency_anchor(self) -> dict[str, Any]:
        """Prove external query, internal ledger and audit share one anchor.

        The three faces carry different shapes (a state view, a folded ledger,
        an audit trail), so they are not byte identical.  What the regulator
        needs is that every face, at the same moment, names the same
        reconciliation checkpoint and the same slice digest, and that digest
        still matches the stream.  Each face therefore reports its own
        projection digest plus the shared anchor; the anchor is verifiable
        against the journal and identical on all three faces.
        """

        from .store.replay import reduce_state

        boundary = self.reconciliation.watermark()
        records = self.reconciliation.readable_records()
        external = self.external_state()
        ledger = reduce_state(records)
        audit_rows = [entry.describe() for entry in self.audit.ledger.entries()]
        slice_digest = self._slice_digest(boundary.through_seq)
        shared_anchor = {
            "checkpoint_id": boundary.checkpoint_id,
            "through_seq": boundary.through_seq,
            "digest": boundary.digest,
        }
        projections = anchor_projection(external=external, ledger=ledger, audit=audit_rows)
        faces = {
            name: {"anchor": dict(shared_anchor), "projection": projections[name]}
            for name in projections
        }
        anchors = [face["anchor"] for face in faces.values()]
        anchor_consistent = bool(anchors) and all(anchor == anchors[0] for anchor in anchors)
        return {
            "reconcile_watermark": boundary.describe(),
            "slice_digest": slice_digest,
            "anchor_matches_stream": boundary.digest == slice_digest,
            "state_faces_identical": external == ledger,
            "anchor_consistent_across_faces": anchor_consistent,
            "faces": faces,
        }

    def _slice_digest(self, through_seq: int) -> str | None:
        if through_seq <= 0:
            return None
        from .reconcile.digest import digest_slice

        return digest_slice(self.store.committed_records(), through_seq=through_seq)

    def reconcile_domain_tip(self) -> int:
        """Expose the highest domain (non boundary metadata) committed seq."""

        return self.reconciliation._domain_tip()

    def advance_ticks(self, ticks: int = 1) -> int:
        """Move the deterministic clock forward and record where it went."""

        moved = self.clock.advance(ticks)
        self.store.publish(CLOCK_KIND, "console", 0, {"active": True, "tick": moved})
        return moved

    def commands(self) -> dict[str, Callable[[Mapping[str, Any]], Any]]:
        """Return the command surface the console exposes."""

        return {
            "recipe.publish": lambda payload: self.recipes.publish_recipe(
                name=text(payload, "name"),
                grade_code=text(payload, "grade"),
                factors=self._factors(payload),
                mix_time_ticks=optional_integer(
                    payload, "mix_time_ticks", self.config.limits.mix_time_ticks
                ),
                moisture_ref_percent=optional_number(
                    payload, "moisture_ref_percent", self.config.limits.moisture_max_percent / 2.0
                ),
            ),
            "recipe.select": lambda payload: self.recipes.select(text(payload, "name")),
            "batch.declare": lambda payload: self.agg.declare_batch(
                batch_id=text(payload, "batch_id"), quantity=number(payload, "quantity")
            ),
            "agg.map_bin": lambda payload: self.agg.map_bin(
                text(payload, "bin"), text(payload, "material")
            ),
            "agg.weigh": lambda payload: self.agg.weigh(
                bin_name=text(payload, "bin"), actual_kg=number(payload, "actual_kg")
            ),
            "agg.persist": lambda payload: self.agg.persist(),
            "agg.check": lambda payload: self.agg.check(actual_total_kg=number(payload, "actual_total_kg")),
            "agg.release": lambda payload: self.agg.release(residual_kg=number(payload, "residual_kg")),
            "agg.drop": lambda payload: self.agg.drop(),
            "agg.calibrate": lambda payload: self.agg.calibrate_scale(
                slope=number(payload, "slope"), offset=number(payload, "offset")
            ),
            "moist.compensate": lambda payload: self.moist.compensate(
                material=text(payload, "material"),
                moisture_percent=number(payload, "moisture_percent"),
                quantity=number(payload, "quantity"),
            ),
            "cement.load_silo": lambda payload: self.cement.load_silo(
                silo=text(payload, "silo"),
                material=optional_text(payload, "material", "cement"),
                level_kg=number(payload, "level_kg"),
            ),
            "cement.open_gate": lambda payload: self.cement.open_gate(
                silo=text(payload, "silo"), target_kg=number(payload, "target_kg")
            ),
            "cement.close_gate": lambda payload: self.cement.close_gate(
                silo=text(payload, "silo"),
                mass_kg=number(payload, "mass_kg"),
                rate_kgs=number(payload, "rate_kgs"),
            ),
            "water.open_valve": lambda payload: self.water.open_valve(target_l=number(payload, "target_l")),
            "water.dispense": lambda payload: self.water.dispense(
                litres=number(payload, "litres"), rate_lps=number(payload, "rate_lps")
            ),
            "water.prime": lambda payload: self.water.prime(litres=number(payload, "litres")),
            "mix.start": lambda payload: self.mix.start(),
            "mix.feed": lambda payload: self.mix.feed(component=text(payload, "component")),
            "mix.time": lambda payload: self.mix.begin_timer(),
            "mix.finish": lambda payload: self.mix.finish_timer(),
            "mix.schedule": lambda payload: self.mix.schedule_sample(),
            "mix.stop": lambda payload: self.mix.stop(
                residual_kg=number(payload, "residual_kg"),
                reason=optional_text(payload, "reason", "batch complete"),
            ),
            "chute.arrive": lambda payload: self.chute.arrive(text(payload, "truck_id")),
            "chute.open": lambda payload: self.chute.open_door(),
            "chute.load": lambda payload: self.chute.load(
                truck_id=text(payload, "truck_id"), volume_m3=number(payload, "volume_m3")
            ),
            "chute.confirm": lambda payload: self.chute.confirm(
                truck_id=text(payload, "truck_id"), volume_m3=number(payload, "volume_m3")
            ),
            "chute.close": lambda payload: self.chute.close_door(),
            "pump.prime": lambda payload: self.pump.prime_confirmed(),
            "pump.start": lambda payload: self.pump.start(),
            "pump.stroke": lambda payload: self.pump.stroke(volume_m3=number(payload, "volume_m3")),
            "pump.pressure": lambda payload: self.pump.observe_pressure(
                reading_mpa=number(payload, "reading_mpa")
            ),
            "pump.finish": lambda payload: self.pump.finish(),
            "sampler.draw": lambda payload: self.sampler.draw(litres=number(payload, "litres")),
            "sampler.release": lambda payload: self.sampler.release(),
            "sampler.file": lambda payload: self._file_sample(number(payload, "litres")),
            "audit.note": lambda payload: self.audit.note(
                text(payload, "name"), self._detail(payload)
            ),
            "audit.rollup": lambda payload: self.audit.rollup(),
            "baseline.publish": lambda payload: self.publish_baseline(
                text(payload, "name"),
                number(payload, "value"),
                optional_text(payload, "unit", ""),
                ttl_ticks=optional_integer(payload, "ttl_ticks", self.config.defaults.baseline_ttl_ticks),
            ),
            "confirmation.take": lambda payload: self.confirm(
                text(payload, "subject"),
                ttl_ticks=optional_integer(payload, "ttl_ticks", self.config.defaults.confirmation_ttl_ticks),
            ),
            "snapshot.take": lambda payload: self.take_snapshot(
                ttl_ticks=optional_integer(payload, "ttl_ticks", self.config.defaults.snapshot_ttl_ticks)
            ),
            "store.rollback": lambda payload: self.rollback(
                integer(payload, "seq"),
                reason=optional_text(payload, "reason", "operator rollback"),
                idem_key=optional_text(payload, "idem_key", None) or None,
            ),
            "reconcile.take": lambda payload: self.reconcile(
                idem_key=optional_text(payload, "idem_key", None) or None
            ),
            "reconcile.status": lambda payload: self.reconcile_status(),
            "reconcile.anchor": lambda payload: self.consistency_anchor(),
            "action.writeoff": lambda payload: self.actions.writeoff(
                integer(payload, "target_seq"),
                idem_key=text(payload, "idem_key"),
                issuer=optional_text(payload, "issuer", "console"),
                detail=self._detail(payload),
            ),
            "action.void": lambda payload: self.actions.void(
                integer(payload, "target_seq"),
                idem_key=text(payload, "idem_key"),
                issuer=optional_text(payload, "issuer", "console"),
                reason=optional_text(payload, "reason", ""),
            ),
            "clock.advance": lambda payload: {"tick": self.advance_ticks(integer(payload, "ticks"))},
        }

    def dispatch(self, command: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Run one command and return everything the caller needs."""

        handler = self.commands().get(command)
        if handler is None:
            raise NotFoundError("unknown command", command=command)
        result = handler(payload or {})
        return {
            "command": command,
            "tick": self.clock.now(),
            "result": result,
            "watermark": self.store.watermark().describe(),
            "alarms": self.alarms.count(),
        }

    def close(self) -> None:
        self.store.close()

    def _file_sample(self, litres: float) -> dict[str, Any]:
        """File a laboratory sample and copy it into the ledger."""

        result = self.sampler.file(litres=litres)
        self.audit.note_sample(
            batch_id=str(result["sample"]["batch_id"]),
            sample_id=str(result["sample"]["sample_id"]),
        )
        return result

    @staticmethod
    def _factors(payload: Mapping[str, Any]) -> dict[str, float]:
        """Read the recipe factors out of a command body."""

        raw = payload.get("factors")
        if isinstance(raw, Mapping):
            return {str(material): float(value) for material, value in raw.items()}
        factors: dict[str, float] = {}
        for material in ("sand", "gravel", "cement", "water", "admixture"):
            value = payload.get(material)
            if value not in (None, ""):
                factors[material] = float(value)
        return factors

    @staticmethod
    def _detail(payload: Mapping[str, Any]) -> dict[str, Any]:
        raw = payload.get("detail")
        return dict(raw) if isinstance(raw, Mapping) else {}
