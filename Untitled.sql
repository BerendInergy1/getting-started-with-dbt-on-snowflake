select *
from table(information_schema.query_history())
order by start_time desc
-- where query_id = '01c138f8-0002-9716-0002-e8ae000491fe'
;

drop view tasty_bytes_dbt_db.dev.raw_pos_franchise;

select top 100 *
from tasty_bytes_dbt_db.dev.tst_snapshot_deletes;