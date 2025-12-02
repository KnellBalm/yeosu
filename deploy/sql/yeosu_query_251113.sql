-- ####################
-- [1] 세대별 세대원 수
-- ####################
with household AS (
    SELECT
        head.jumin_head_sid,
        COUNT(DISTINCT mbr.jumin_sid) AS member_count
    FROM tb_gmc_hshldr_info head
             LEFT JOIN tb_gmc_fmbr_info mbr
                       ON head.jumin_head_sid = mbr.jumin_head_sid
    WHERE head.jumin_state_code IN ('10', '13', '43')
      AND head.data_crtr_dt = (select MAX(data_crtr_dt) from tb_gmc_hshldr_info) -- #변수#
    GROUP BY head.jumin_head_sid
)
SELECT
    household.jumin_head_sid,
    household.member_count, -- 세대원 수
    head.jumin_head_sid_sno, -- 세대주주민일련번호
    head.jumin_regn_code, -- 법정동코드
    head.jumin_rd_code, -- 도로명 코드
    head.jumin_san, -- 산여부
    head.jumin_bdng_orgno, -- 건물 본번 
    head.jumin_bdng_subno, -- 건물 부번
    head.data_crtr_dt
from household
LEFT JOIN tb_gmc_hshldr_info head ON household.jumin_head_sid = head.jumin_head_sid
WHERE head.data_crtr_dt = (select MAX(data_crtr_dt) from tb_gmc_hshldr_info)
; 


-- ####################
-- [2] 전입자 쿼리
-- ####################
SELECT
    mvin.jumin_sid,
    -- 성별 계산 (7번째 자리)
    CASE
        WHEN CAST(SUBSTRING(mvin.jumin_sid, 7, 1) AS INTEGER) % 2 = 1 THEN 'M'
        ELSE 'F'
        END AS gender,
    -- 나이 계산: 현재년도(두 자리) - 주민번호 앞 두 자리 → 음수면 +100
    CASE
        WHEN ((CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100) - CAST(SUBSTRING(mvin.jumin_sid, 1, 2) AS INTEGER)) < 0
            THEN ((CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100) - CAST(SUBSTRING(mvin.jumin_sid, 1, 2) AS INTEGER) + 100)
        ELSE ((CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100) - CAST(SUBSTRING(mvin.jumin_sid, 1, 2) AS INTEGER))
        END AS age,
    mvin.jumin_inr_rd_code,
    mvin.jumin_inr_regn_code,
    mvin.jumin_inr_bdng_orgno,
    mvin.jumin_inr_bdng_subno,
    mvin.jumin_inr_san
FROM tb_gmc_mvin_info mvin
where mvin.data_crtr_dt = (select MAX(data_crtr_dt) from tb_gmc_mvin_info)
;

-- ####################
-- [3] 전출자 쿼리
-- ####################
SELECT
    mvout.jumin_sid,
    -- 성별 계산 (7번째 자리)
    CASE
        WHEN CAST(SUBSTRING(mvout.jumin_sid, 7, 1) AS INTEGER) % 2 = 1 THEN 'M'
        ELSE 'F'
        END AS gender,
    -- 나이 계산: 현재년도(두 자리) - 주민번호 앞 두 자리 → 음수면 +100
    CASE
        WHEN ((CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100) - CAST(SUBSTRING(mvout.jumin_sid, 1, 2) AS INTEGER)) < 0
            THEN ((CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100) - CAST(SUBSTRING(mvout.jumin_sid, 1, 2) AS INTEGER) + 100)
        ELSE ((CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100) - CAST(SUBSTRING(mvout.jumin_sid, 1, 2) AS INTEGER))
        END AS age,
    mvout.jumin_inr_rd_code,
    mvout.jumin_exr_rd_code,
    mvout.jumin_exr_regn_code,
    mvout.jumin_exr_bdng_orgno,
    mvout.jumin_exr_bdng_subno,
    mvout.jumin_exr_san
FROM tb_gmc_mvout_info mvout
where mvout.data_crtr_dt = (select MAX(data_crtr_dt) from tb_gmc_mvout_info)
;

-- ####################
-- [4] 인구(세대원) 쿼리
-- ####################
SELECT
    mbr.jumin_sid,
    -- 성별 계산 (7번째 자리) - 세대원 기준
    CASE
        WHEN CAST(SUBSTRING(mbr.jumin_sid, 7, 1) AS INTEGER) % 2 = 1 THEN 'M'
        ELSE 'F'
    END AS gender,
    -- 나이 계산: 세대원 주민번호 기준
    CASE
        WHEN (
            (CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100)
            - CAST(SUBSTRING(mbr.jumin_sid, 1, 2) AS INTEGER)
        ) < 0
        THEN (
            (CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100)
            - CAST(SUBSTRING(mbr.jumin_sid, 1, 2) AS INTEGER)
            + 100
        )
        ELSE (
            (CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100)
            - CAST(SUBSTRING(mbr.jumin_sid, 1, 2) AS INTEGER)
        )
    END AS age,
    head.jumin_head_sid,
    head.jumin_inport_ymd as "jumin_head_inport_ymd", -- 전입일자
    head.jumin_rd_code,    -- 도로명 코드
    head.jumin_regn_code,  -- 법정동코드
    head.jumin_bdng_orgno, -- 건물 본번 
    head.jumin_bdng_subno, -- 건물 부번
    head.jumin_san,
    mbr.data_crtr_dt
FROM tb_gmc_fmbr_info mbr
LEFT JOIN tb_gmc_hshldr_info head 
    ON head.jumin_head_sid = mbr.jumin_head_sid
   AND head.data_crtr_dt = (SELECT MAX(data_crtr_dt) FROM tb_gmc_hshldr_info)
WHERE mbr.jumin_state_code IN ('10', '13', '43')
  AND mbr.data_crtr_dt = (SELECT MAX(data_crtr_dt) FROM tb_gmc_fmbr_info)
;

