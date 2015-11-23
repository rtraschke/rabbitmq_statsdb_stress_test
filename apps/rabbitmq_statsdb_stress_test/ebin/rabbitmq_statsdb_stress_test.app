{ application, rabbitmq_statsdb_stress_test, [
    { description, "Rabbit MQ Stats DB Stress Test" },
    { vsn, 0.2 },
    { modules, [
        rabbit_mgmt_db_stressor,
        rabbit_mgmt_db_stress_stats
    ] },
    { registered, [] },
    { applications, [
        kernel,
        stdlib
    ] },
    { env, [] }
] }.
