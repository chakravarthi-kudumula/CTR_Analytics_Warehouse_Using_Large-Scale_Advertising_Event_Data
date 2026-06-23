#!/usr/bin/env python3

"""Promote only the best scheduled or candidate model run into the active canonical slot."""

from __future__ import annotations

import argparse

from pipeline_tracking import (
    complete_pipeline_run,
    complete_pipeline_step,
    create_pipeline_run,
    create_pipeline_step,
    ensure_pipeline_metadata,
    fetch_batch_details,
)
from project_config import (
    ML_DEFAULT_MODEL_BASE_VERSION,
    ML_DEFAULT_MODEL_NAME,
    PROJECT_ROOT,
    SQL_DIR,
    add_db_connection_args,
)
from train_ctr_baseline import connect


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote a candidate model to active canonical status")
    parser.add_argument("--model-name", default=ML_DEFAULT_MODEL_NAME)
    parser.add_argument("--candidate-version", default=ML_DEFAULT_MODEL_BASE_VERSION)
    parser.add_argument("--candidate-training-run-id", type=int)
    parser.add_argument("--min-roc-auc-improvement", type=float, default=0.005)
    parser.add_argument("--min-pr-auc-improvement", type=float, default=0.005)
    parser.add_argument("--min-lift-improvement", type=float, default=0.05)
    parser.add_argument("--bootstrap-metadata", action="store_true")
    parser.add_argument("--triggered-by", default="manual")
    add_db_connection_args(parser)
    return parser.parse_args()


def fetch_candidate(connection, *, model_name: str, candidate_version: str, candidate_training_run_id: int | None) -> dict[str, object]:
    base_query = """
        select
            mr.model_id,
            mr.model_name,
            mr.model_version,
            tr.training_run_id,
            tr.train_batch_name,
            tr.rows_trained,
            mcs.validation_roc_auc,
            mcs.validation_pr_auc,
            mcs.validation_lift_at_10pct
        from ml.model_registry mr
        join ml.training_runs tr
          on tr.model_id = mr.model_id
         and tr.run_status = 'SUCCESS'
        join ml.model_comparison_summary mcs
          on mcs.model_name = mr.model_name
         and mcs.model_version = mr.model_version
         and mcs.training_run_id = tr.training_run_id
        where mr.model_name = %s
          and mr.model_version = %s
    """
    params: tuple[object, ...]
    if candidate_training_run_id is None:
        query = base_query + " order by tr.training_run_id desc limit 1;"
        params = (model_name, candidate_version)
    else:
        query = base_query + " and tr.training_run_id = %s order by tr.training_run_id desc limit 1;"
        params = (model_name, candidate_version, candidate_training_run_id)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()
    if not row:
        raise ValueError(f"No successful candidate run found for {model_name} {candidate_version}")
    return {
        "model_id": int(row[0]),
        "model_name": str(row[1]),
        "model_version": str(row[2]),
        "training_run_id": int(row[3]),
        "train_batch_name": str(row[4]),
        "rows_trained": int(row[5]) if row[5] is not None else 0,
        "validation_roc_auc": float(row[6]) if row[6] is not None else None,
        "validation_pr_auc": float(row[7]) if row[7] is not None else None,
        "validation_lift_at_10pct": float(row[8]) if row[8] is not None else None,
    }


def fetch_active_canonical(connection, *, model_name: str) -> dict[str, object] | None:
    query = """
        select
            mr.model_id,
            mr.model_name,
            mr.model_version,
            tr.training_run_id,
            tr.train_batch_name,
            tr.rows_trained,
            mcs.validation_roc_auc,
            mcs.validation_pr_auc,
            mcs.validation_lift_at_10pct
        from ml.model_registry mr
        left join ml.training_runs tr
          on tr.model_id = mr.model_id
         and tr.run_status = 'SUCCESS'
        left join ml.model_comparison_summary mcs
          on mcs.model_name = mr.model_name
         and mcs.model_version = mr.model_version
         and mcs.training_run_id = tr.training_run_id
        where mr.model_name = %s
          and mr.is_active_canonical = true
        order by
            mr.canonical_promoted_at desc nulls last,
            tr.training_run_id desc nulls last
        limit 1;
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (model_name,))
        row = cursor.fetchone()
    if not row:
        return None
    return {
        "model_id": int(row[0]),
        "model_name": str(row[1]),
        "model_version": str(row[2]),
        "training_run_id": int(row[3]),
        "train_batch_name": str(row[4]),
        "rows_trained": int(row[5]) if row[5] is not None else 0,
        "validation_roc_auc": float(row[6]) if row[6] is not None else None,
        "validation_pr_auc": float(row[7]) if row[7] is not None else None,
        "validation_lift_at_10pct": float(row[8]) if row[8] is not None else None,
    }


def fetch_base_version_reference(connection, *, model_name: str) -> dict[str, object] | None:
    return fetch_candidate(
        connection,
        model_name=model_name,
        candidate_version=ML_DEFAULT_MODEL_BASE_VERSION,
        candidate_training_run_id=None,
    )


def normalize_active_canonical(connection, *, model_name: str) -> None:
    query = """
        with ranked_active as (
            select
                mr.model_id,
                row_number() over (
                    order by
                        coalesce(mcs.validation_roc_auc, 0) desc,
                        coalesce(mcs.validation_pr_auc, 0) desc,
                        coalesce(mcs.validation_lift_at_10pct, 0) desc,
                        case when mr.model_version = %s then 0 else 1 end,
                        mr.canonical_promoted_at desc nulls last,
                        tr.training_run_id desc nulls last
                ) as active_rank
            from ml.model_registry mr
            left join ml.training_runs tr
              on tr.model_id = mr.model_id
             and tr.run_status = 'SUCCESS'
            left join ml.model_comparison_summary mcs
              on mcs.model_name = mr.model_name
             and mcs.model_version = mr.model_version
             and mcs.training_run_id = tr.training_run_id
            where mr.model_name = %s
              and mr.is_active_canonical = true
        )
        update ml.model_registry mr
        set is_active_canonical = case when ranked_active.active_rank = 1 then true else false end
        from ranked_active
        where mr.model_id = ranked_active.model_id;
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (ML_DEFAULT_MODEL_BASE_VERSION, model_name))


def evaluate_promotion(
    candidate: dict[str, object],
    active: dict[str, object] | None,
    base_reference: dict[str, object] | None,
    args: argparse.Namespace,
) -> tuple[bool, str]:
    if candidate["rows_trained"] < 500000:
        return False, "Candidate run is not large-scale enough for canonical promotion."
    if candidate["validation_roc_auc"] is None or candidate["validation_pr_auc"] is None or candidate["validation_lift_at_10pct"] is None:
        return False, "Candidate run does not have complete validation and ranking metrics."
    if (
        candidate.get("model_version") != ML_DEFAULT_MODEL_BASE_VERSION
        and base_reference
        and base_reference.get("training_run_id") != candidate.get("training_run_id")
        and (base_reference.get("validation_roc_auc") or 0.0) >= (candidate.get("validation_roc_auc") or 0.0)
        and (base_reference.get("validation_pr_auc") or 0.0) >= (candidate.get("validation_pr_auc") or 0.0)
        and (base_reference.get("validation_lift_at_10pct") or 0.0) >= (candidate.get("validation_lift_at_10pct") or 0.0)
    ):
        return False, "Scheduled candidate does not beat the canonical base version on ROC-AUC, PR-AUC, and lift."
    if active is None:
        return True, "No active canonical model exists yet; promoting the first large-scale qualified model."
    if active["training_run_id"] == candidate["training_run_id"]:
        return False, "Candidate is already the active canonical training run."

    if (
        candidate.get("model_version") == ML_DEFAULT_MODEL_BASE_VERSION
        and active.get("model_version") != ML_DEFAULT_MODEL_BASE_VERSION
        and abs((candidate["validation_roc_auc"] or 0.0) - (active["validation_roc_auc"] or 0.0)) < 1e-9
        and abs((candidate["validation_pr_auc"] or 0.0) - (active["validation_pr_auc"] or 0.0)) < 1e-9
        and abs((candidate["validation_lift_at_10pct"] or 0.0) - (active["validation_lift_at_10pct"] or 0.0)) < 1e-9
    ):
        return True, "Candidate matches the scheduled clone on metrics and reclaims the canonical base version slot."

    roc_ok = candidate["validation_roc_auc"] >= (active["validation_roc_auc"] or 0.0) + args.min_roc_auc_improvement
    pr_ok = candidate["validation_pr_auc"] >= (active["validation_pr_auc"] or 0.0) + args.min_pr_auc_improvement
    lift_ok = candidate["validation_lift_at_10pct"] >= (active["validation_lift_at_10pct"] or 0.0) + args.min_lift_improvement

    if roc_ok and pr_ok and lift_ok:
        return True, "Candidate exceeded the active canonical model on ROC-AUC, PR-AUC, and lift thresholds."

    reasons = []
    if not roc_ok:
        reasons.append("ROC-AUC threshold not met")
    if not pr_ok:
        reasons.append("PR-AUC threshold not met")
    if not lift_ok:
        reasons.append("lift threshold not met")
    return False, "; ".join(reasons) + "."


def record_promotion_audit(connection, *, model_name: str, candidate: dict[str, object], active: dict[str, object] | None, promoted: bool, reason: str) -> None:
    query = """
        insert into ml.model_promotion_audit (
            model_name,
            candidate_model_id,
            candidate_training_run_id,
            previous_model_id,
            previous_training_run_id,
            promotion_decision,
            decision_reason,
            candidate_validation_roc_auc,
            candidate_validation_pr_auc,
            candidate_validation_lift_at_10pct,
            previous_validation_roc_auc,
            previous_validation_pr_auc,
            previous_validation_lift_at_10pct
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                model_name,
                candidate["model_id"],
                candidate["training_run_id"],
                active["model_id"] if active else None,
                active["training_run_id"] if active else None,
                "PROMOTED" if promoted else "REJECTED",
                reason,
                candidate["validation_roc_auc"],
                candidate["validation_pr_auc"],
                candidate["validation_lift_at_10pct"],
                active["validation_roc_auc"] if active else None,
                active["validation_pr_auc"] if active else None,
                active["validation_lift_at_10pct"] if active else None,
            ),
        )


def apply_promotion(connection, *, candidate: dict[str, object], reason: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            update ml.model_registry
            set is_active_canonical = false
            where model_name = %s
              and is_active_canonical = true;
            """,
            (candidate["model_name"],),
        )
        cursor.execute(
            """
            update ml.model_registry
            set is_active_canonical = true,
                canonical_promoted_at = now(),
                canonical_promotion_note = %s
            where model_id = %s;
            """,
            (reason, candidate["model_id"]),
        )


def main() -> None:
    args = parse_args()
    if args.bootstrap_metadata:
        ensure_pipeline_metadata(SQL_DIR, args.database, args)

    with connect(args) as connection:
        normalize_active_canonical(connection, model_name=args.model_name)
        connection.commit()
        candidate = fetch_candidate(
            connection,
            model_name=args.model_name,
            candidate_version=args.candidate_version,
            candidate_training_run_id=args.candidate_training_run_id,
        )
        active = fetch_active_canonical(connection, model_name=args.model_name)
        base_reference = fetch_base_version_reference(connection, model_name=args.model_name)

    batch_details = fetch_batch_details(candidate["train_batch_name"], args.database, args)
    batch_id = int(batch_details["batch_id"]) if batch_details else None
    source_file = str(batch_details["source_file"]) if batch_details else None

    pipeline_run_id = create_pipeline_run(
        batch_id=batch_id,
        pipeline_name="ml_model_promotion",
        layer_name="ml",
        source_file=source_file,
        triggered_by=args.triggered_by,
        database=args.database,
        args=args,
    )
    pipeline_step_id = create_pipeline_step(
        pipeline_run_id=pipeline_run_id,
        batch_id=batch_id,
        step_name="promote_canonical_model",
        layer_name="ml",
        target_table="ml.model_registry",
        source_file=source_file,
        database=args.database,
        args=args,
    )

    try:
        promoted, reason = evaluate_promotion(candidate, active, base_reference, args)
        with connect(args) as connection:
            if promoted:
                apply_promotion(connection, candidate=candidate, reason=reason)
            record_promotion_audit(
                connection,
                model_name=args.model_name,
                candidate=candidate,
                active=active,
                promoted=promoted,
                reason=reason,
            )
            connection.commit()

        status = "SUCCESS" if promoted else "SKIPPED"
        step_message = f"{'Promoted' if promoted else 'Did not promote'} {candidate['model_name']} {candidate['model_version']}: {reason}"
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status=status,
            rows_processed=1,
            step_message=step_message,
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status=status,
            run_message=step_message,
            database=args.database,
            args=args,
        )

        print(f"Project root: {PROJECT_ROOT}")
        print(f"Candidate model: {candidate['model_name']} {candidate['model_version']}")
        print(f"Candidate training run id: {candidate['training_run_id']}")
        print(f"Promotion decision: {'PROMOTED' if promoted else 'REJECTED'}")
        print(f"Reason: {reason}")
    except Exception as exc:
        complete_pipeline_step(
            pipeline_step_id=pipeline_step_id,
            step_status="FAILED",
            rows_processed=None,
            step_message=str(exc),
            database=args.database,
            args=args,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            run_status="FAILED",
            run_message=str(exc),
            database=args.database,
            args=args,
        )
        raise


if __name__ == "__main__":
    main()
