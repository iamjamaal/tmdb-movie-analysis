#!/usr/bin/env python3
import json
from airflow.models import XCom
from airflow.utils.session import create_session


def get_xcom(dag_id, task_id, run_id, key):
    with create_session() as session:
        xc = session.query(XCom).filter(
            XCom.dag_id == dag_id,
            XCom.task_id == task_id,
            XCom.run_id == run_id,
            XCom.key == key,
        ).one_or_none()
        if xc is None:
            return None
        try:
            return xc.value
        except Exception:
            return repr(xc.value)


if __name__ == '__main__':
    dag = 'tmdb_movie_pipeline'
    run = 'manual__2026-01-25T14:14:37+00:00'
    task = 'generate_visualizations'
    keys = ['visualizations', 'interactive_plotly']
    out = {}
    for k in keys:
        out[k] = get_xcom(dag, task, run, k)
    print(json.dumps(out, indent=2))
