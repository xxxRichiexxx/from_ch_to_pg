clickhouse-client --host={{ params.host }} --port=9000 \
--user={{ params.login }} --password={{ params.password }} \
--query="{{ dag_run.conf['src_query'] }} INTO OUTFILE '{{ params.base_dir }}/ch/{{ dag_run.conf['dst_table'] }}.csv' FORMAT CSV;"