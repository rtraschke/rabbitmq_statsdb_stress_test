RABBIT=$1

erl -pa apps/*/ebin deps/*/ebin \
	-noinput \
	-sname clear_tracer \
	-run rabbit_mgmt_db_stressor clear_tracer $RABBIT \
	-run init stop

