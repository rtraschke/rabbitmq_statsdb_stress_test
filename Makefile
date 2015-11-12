.PHONY: all init compile rules rel rmrel package clean distclean start stop console rmlog ec2start test dialyze

all: init compile

# All the static upfront dependencies we need for the project
init: deps scripts \
	deps/bear \
	deps/amqp_client \
	deps/rabbit_common


####
# Compile Erlang sources by generating the Emakefile and invoking
# make:all/1 . A small wrapper (ERL_MAKE) to generate make compatible
# exit codes is needed for this to work.
#
# The Emakefile is generated to include all .erl files found in the
# apps/*/src subdirectories (ERL_SOURCES) and write the .beam files
# into the respective ebin directories.
#
# The common compilation options (ERL_MAKE_OPTS) are passed in via
# the make:all/1 call.
####

ERL_SOURCES=$(shell ls apps/*/src/*.erl) $(shell ls apps/*/include/*.hrl)

Emakefile: $(ERL_SOURCES)
	ls apps |awk '{ print "{[\"apps/" $$1 "/src/*\"], [{outdir, \"apps/" $$1 "/ebin\"}]}." }' >Emakefile

ERL_MAKE=case make:all([ $(ERL_MAKE_OPTS) ]) of up_to_date -> halt(0); error -> halt(1) end.

ERL_MAKE_OPTS=debug_info, report, {i, "deps"}, {i, "apps"}

compile: Emakefile
	erl -noinput -pa deps/*/ebin -eval '$(ERL_MAKE)'


# Cleanup

clean:
	-rm apps/*/ebin/*.beam

distclean: clean
	-rm -rf deps
	-rm .dialyzer_plt


# Run dialyzer, the Erlang type checker

dialyze: .dialyzer_plt compile
	dialyzer --plt .dialyzer_plt -Wrace_conditions -I deps apps/*/ebin

.dialyzer_plt:
	dialyzer --build_plt --output_plt .dialyzer_plt --apps erts kernel stdlib runtime_tools -r deps


####
# Dependencies and other stuff required to get us going
#
# Some make specific vaiables get used in the recipes below:
#
# $@ - the target
# $(@F) - the filename part of the target (i.e. the basename)
####

PWD=$(shell pwd)

REBAR=$(PWD)/scripts/rebar

scripts:
	mkdir scripts

deps:
	mkdir deps

# Fetch rebar for building deps

scripts/rebar:
	# Must follow redirects (-L), since github returns those
	curl -L -o $@ https://github.com/rebar/rebar/releases/download/2.6.1/rebar
	chmod +x $@

# Dependencies from github

deps/bear: scripts/rebar
	git clone -n -- https://github.com/boundary/bear $@
	(cd $@ && git checkout -q 0.8.2 && $(REBAR) compile)

# Dependencies from rabbitmq

RABBITMQ_VERSION=3.5.6

deps/amqp_client deps/rabbit_common: deps/amqp_client-$(RABBITMQ_VERSION).ez deps/rabbit_common-$(RABBITMQ_VERSION).ez
	(cd deps && unzip $(@F)-$(RABBITMQ_VERSION).ez)
	-rm -r $@
	mv $@-$(RABBITMQ_VERSION) $@
	touch $@

deps/amqp_client-$(RABBITMQ_VERSION).ez deps/rabbit_common-$(RABBITMQ_VERSION).ez:
	curl -o $@ http://www.rabbitmq.com/releases/rabbitmq-erlang-client/v$(RABBITMQ_VERSION)/$(@F)

