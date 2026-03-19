-- FlyWire Standard Schema
CREATE NODE TABLE Neuron (
    root_id         INT64 PRIMARY KEY,
    group           STRING,
    side            STRING,
    flow            STRING,
    super_class     STRING,
    class           STRING,
    sub_class       STRING,
    nt_type         STRING,
    ach_avg         DOUBLE,
    gaba_avg        DOUBLE,
    glut_avg        DOUBLE,
    input_cells     INT64,
    output_cells    INT64,
    input_synapses  INT64,
    output_synapses INT64,
    length_nm       DOUBLE,
    area_nm         DOUBLE,
    size_nm         DOUBLE
);

CREATE REL TABLE SYNAPSE (
    FROM Neuron TO Neuron,
    neuropil        STRING,
    syn_count       INT64,
    nt_type         STRING,
    ach_avg         DOUBLE,
    gaba_avg        DOUBLE,
    glut_avg        DOUBLE
);
