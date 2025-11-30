-- cpp_z_sl
update dma_pre.cpp_z_sl t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_dispans_sl
update dma_pre.cpp_dispans_sl t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_link
update dma_pre.cpp_link t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_lek
update dma_pre.cpp_lek t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_med_dev
update dma_pre.cpp_med_dev t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_napr
update dma_pre.cpp_napr t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_onk_sl
update dma_pre.cpp_onk_sl t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_onk_usl
update dma_pre.cpp_onk_usl t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_pall
update dma_pre.cpp_pall t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_usl
update dma_pre.cpp_usl t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;

-- cpp_vmp
update dma_pre.cpp_vmp t
set update_dttm = now(),
    master_person_id = upd.master_person_id
from dma_calc.cpp_master_person_upd_{{params.temp_suffix_run_id}} upd
where upd.old_master_person_id = t.master_person_id;