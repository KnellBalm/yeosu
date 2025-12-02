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
      AND head.data_crtr_dt = '20250305000000' -- #변수#
    GROUP BY head.jumin_head_sid
)
SELECT
    household.jumin_head_sid,
    household.member_count, -- 세대원 수
    head.jumin_head_sid_sno, -- 세대주주민일련번호
    head.jumin_admdng_code, -- 행정동 코드
    head.jumin_regn_code, -- 법정동 코드
    head.jumin_inport_ymd, -- 전입일자
    head.jumin_san, -- 산
    head.jumin_bunji, -- 번지
    head.jumin_ho, -- 호
    head.jumin_rd_code, -- 도로명 코드
    head.jumin_bdng_orgno, -- 건물 본번
    head.jumin_bdng_subno -- 건물 부번
    from household
LEFT JOIN tb_gmc_hshldr_info head ON household.jumin_head_sid = head.jumin_head_sid
    WHERE head.data_crtr_dt = '20250305000000'; -- #변수#


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
    mvin.jumin_sid_sno,
    mvin.jumin_umd_ipt_ymd,
    mvin.jumin_cgg_recv_sno,
    mvin.jumin_inr_admdng_code,
    mvin.jumin_inr_regn_code,
    mvin.jumin_exr_admdng_code,
    mvin.jumin_inport_ymd,
    mvin.data_crtr_dt,
    mvin.jumin_inr_san,
    mvin.jumin_inr_bunji,
    mvin.jumin_inr_ho,
    mvin.jumin_inr_rd_code,
    mvin.jumin_inr_bdng_orgno,
    mvin.jumin_inr_bdng_subno
FROM tb_gmc_mvin_info mvin
where mvin.data_crtr_dt = '20250305000000';

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
    mvout.jumin_sid_sno,
    mvout.jumin_sid_sno,
    mvout.jumin_umd_ipt_ymd,
    mvout.jumin_cgg_recv_sno,
    mvout.jumin_inr_admdng_code,
    mvout.jumin_inr_regn_code,
    mvout.jumin_exr_admdng_code,
    mvout.jumin_exr_regn_code,
    mvout.jumin_export_ymd,
    mvout.data_crtr_dt,
    mvout.jumin_inr_san,
    mvout.jumin_inr_bunji,
    mvout.jumin_inr_ho,
    mvout.jumin_inr_rd_code,
    mvout.jumin_inr_bdng_orgno,
    mvout.jumin_inr_bdng_subno,
    mvout.jumin_exr_san,
    mvout.jumin_exr_bunji,
    mvout.jumin_exr_ho,
    mvout.jumin_exr_rd_code,
    mvout.jumin_exr_bdng_orgno,
    mvout.jumin_exr_bdng_subno
FROM tb_gmc_mvout_info mvout
where mvout.data_crtr_dt = '20250305000000';

-- ####################
-- [4] 인구(세대원) 쿼리
-- ####################
SELECT
    mbr.jumin_sid,
    -- 성별 계산 (7번째 자리)
    CASE
        WHEN CAST(SUBSTRING(mbr.jumin_head_sid, 7, 1) AS INTEGER) % 2 = 1 THEN 'M'
        ELSE 'F'
        END AS gender,
    -- 나이 계산: 현재년도(두 자리) - 주민번호 앞 두 자리 → 음수면 +100
    CASE
        WHEN ((CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100) - CAST(SUBSTRING(head.jumin_head_sid, 1, 2) AS INTEGER)) < 0
            THEN ((CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100) - CAST(SUBSTRING(head.jumin_head_sid, 1, 2) AS INTEGER) + 100)
        ELSE ((CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) % 100) - CAST(SUBSTRING(head.jumin_head_sid, 1, 2) AS INTEGER))
        END AS age,
    mbr.jumin_sid_sno,
    mbr.jumin_head_sid,
    mbr.jumin_head_sid_sno, -- 세대주주민일련번호
    mbr.jumin_state_code, -- 상태 코드
    mbr.jumin_head_rel_code, -- 세대주와의 관계 코드
    mbr.jumin_inport_ymd, -- 전입일자
    head.jumin_admdng_code, -- 행정동 코드
    head.jumin_regn_code, -- 법정동 코드
    head.jumin_inport_ymd as "jumin_head_inport_ymd", -- 전입일자
    head.jumin_san, -- 산
    head.jumin_bunji, -- 번지
    head.jumin_ho, -- 호
    head.jumin_rd_code, -- 도로명 코드
    head.jumin_bdng_orgno, -- 건물 본번
    head.jumin_bdng_subno, -- 건물 부번
    mbr.data_crtr_dt
FROM tb_gmc_fmbr_info mbr
LEFT JOIN tb_gmc_hshldr_info head
    ON head.jumin_head_sid = mbr.jumin_head_sid
WHERE mbr.jumin_state_code IN ('10', '13', '43')
  AND mbr.data_crtr_dt = '20250328000000';

