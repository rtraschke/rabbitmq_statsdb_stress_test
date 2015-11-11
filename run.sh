RABBIT=$1

erl -pa apps/*/ebin deps/*/ebin \
	-noinput \
	-sname stress \
	-run rabbit_mgmt_db_stressor run $RABBIT \
	-run init stop
