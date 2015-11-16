RABBIT=$1
CSV=$2

erl -pa apps/*/ebin deps/*/ebin \
	-noinput \
	-sname stress \
	-run rabbit_mgmt_db_stressor run $RABBIT $CSV\
	-run init stop
