RABBIT=$1
CSV=$2
OPTION=$3

erl -pa apps/*/ebin deps/*/ebin \
	-noinput \
	-sname stress \
	-run rabbit_mgmt_db_stressor run $RABBIT $CSV $OPTION\
	-run init stop
