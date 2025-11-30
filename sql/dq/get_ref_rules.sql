select
     id
    ,group_cd
from
    meta.mt_dq_rule
where
    group_cd='REF_STAGE'
    and act_flg=1