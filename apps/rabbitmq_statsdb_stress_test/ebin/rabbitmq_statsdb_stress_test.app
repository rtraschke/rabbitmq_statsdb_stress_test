{ application, rabbitmq_statsdb_stress_test, [
    { description, "Rabbit MQ Stats DB Stress Test" },
    { vsn, 0.1 },
    { modules, [
        rabbit_mgmt_db_stressor
    ] },
    { registered, [] },
    { applications, [
        kernel,
        stdlib
    ] },
    { env, [] }
] }.
