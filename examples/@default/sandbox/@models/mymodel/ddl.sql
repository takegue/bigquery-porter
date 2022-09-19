CREATE MODEL
  `sandbox.mymodel`
OPTIONS
  ( MODEL_TYPE='LINEAR_REG',
    LS_INIT_LEARN_RATE=0.15,
    L1_REG=1,
    MAX_ITERATIONS=5 ) AS
SELECT
  c1 as column1,
  c1 * 2 as column2,
  c1 * 3 as column3,
  c1 * 10 as label
FROM
  unnest(generate_array(1, 10)) c1
