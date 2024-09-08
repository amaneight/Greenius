-- Databricks notebook source

-- VIEW Melbourne Public Transport Patronage Data
select * from vic_ds.monthly_public_transport_patronage_by_mode where `Metropolitan bus`!=''


select concat(Year ,'-', Month) as MonthYear, 
cast(replace(`Metropolitan train`, ',','' ) as double) as MetroTrain, 
cast(replace(`Metropolitan tram`, ',','' ) as double) as MetroTram,
cast(replace(`Metropolitan bus`, ',','' ) as double) as MetroBus,
cast(replace(`Regional train`, ',','' ) as double) as VLineTrain, 
cast(replace(`Regional coach`, ',','' ) as double) as VLineCoach,
cast(replace(`Regional bus`, ',','' ) as double) as VLineBus
from vic_ds.monthly_public_transport_patronage_by_mode where `Metropolitan bus`!=''


-- VIEW Total Vehicles by home suburb region
select sum(totalvehs) , homesubregion_ASGC from vic_ds.households_vista_1220_lga where surveyperiod < '2020-03'
group by homesubregion_ASGC



-- VIEW how many PETROl, DIESEL and EV cars were purchased 2010-2024

select NB_YEAR_MFC_VEH, CD_CL_FUEL_ENG, count(*) from `vic_ds`.`whole_fleet_vehicle_registration_snapshot_by_postcode_q_2_2024_2`
where NB_YEAR_MFC_VEH > 2010 and NB_YEAR_MFC_VEH < 2024 and CD_CL_FUEL_ENG in ('E ','D ','P ')
group by NB_YEAR_MFC_VEH, CD_CL_FUEL_ENG;

-- 

select POSTCODE, CD_CL_FUEL_ENG, count(*) from `vic_ds`.`whole_fleet_vehicle_registration_snapshot_by_postcode_q_2_2024_2`
where NB_YEAR_MFC_VEH > 2010 and NB_YEAR_MFC_VEH < 2024 and CD_CL_FUEL_ENG in ('E ','D ','P ') and 
postcode in (3000,3001,3002,3003,3004,3005,3006,3007,3008,3009,3010,3011,3012,3013,3014,3015,3016,3017,3018,3019,3020,3021,3022,3023,3024,3025,3026,3335,3336,3337,3338,3339,3340,3750,3751,3752,3753,3754,3755,3756,3757,3758,3759,3760,3761,3762,3763,3764,3765,3766,3767,3768,3769,3770,3771,3772,3773,3774,3775,3776,3777,3778,3779,3780,3781,3782,3783,3784,3785,3786,3787,3788,3789,3790,3791,3792,3793,3794,3795,3796,3797,3798,3799,3800,3801,3802,3803,3804,3805,3806,3807,3808,3809,3910,3911,3912,3913,3914,3915,3916,3917,3918,3919,3926,3927,3928,3929,3930,3931,3932,3933,3934,3935,3936,3937,3938,3939,3940,3941,3942,3943,3975,3976,3977,3980,3981,3027,3341,3810,3920,3944,3978,3981)

group by POSTCODE, CD_CL_FUEL_ENG;




-- Update Data set and divide post code by region


update  `vic_ds`.`whole_fleet_vehicle_registration_snapshot_by_postcode_q_2_2024_2` 
set mel_region = CASE WHEN POSTCODE BETWEEN 3000 AND 3006 THEN 'MELBOURNE_CBD' WHEN POSTCODE BETWEEN 3101 AND 3180 THEN 'MELBOURNE_EAST' WHEN POSTCODE BETWEEN 3011 and 3049 THEN 'MELBOURNE_WEST' WHEN POSTCODE BETWEEN 3180 AND 3207 THEN 'MELBOURNE_SOUTHERN' WHEN POSTCODE BETWEEN 3051 AND 3099 THEN 'MELBOURNE_NORTHERN' ELSE NULL END


-- VIEW how many PETROl, DIESEL and EV cars were purchased 

select mel_region, CD_CL_FUEL_ENG, count(*) from `vic_ds`.`whole_fleet_vehicle_registration_snapshot_by_postcode_q_2_2024_2`
where NB_YEAR_MFC_VEH > 2010 and NB_YEAR_MFC_VEH < 2024 and CD_CL_FUEL_ENG in ('E ','D ','P ') and mel_region!='null'
group by  mel_region, CD_CL_FUEL_ENG




-- VIEW how many PETROl, DIESEL and EV cars were purchased every year
select mel_region, NB_YEAR_MFC_VEH, count(*) from `vic_ds`.`whole_fleet_vehicle_registration_snapshot_by_postcode_q_2_2024_2`
where NB_YEAR_MFC_VEH > 2010 and NB_YEAR_MFC_VEH < 2024 and CD_CL_FUEL_ENG in ('E ','D ','P ') and mel_region!='null'
group by  mel_region, NB_YEAR_MFC_VEH
 
select  cast(NB_YEAR_MFC_VEH as string), CD_CL_FUEL_ENG, count(*) from `vic_ds`.`whole_fleet_vehicle_registration_snapshot_by_postcode_q_2_2024_2`
where NB_YEAR_MFC_VEH > 2010 and NB_YEAR_MFC_VEH < 2024 and CD_CL_FUEL_ENG in ('E ','D ','P ') and mel_region!='null'
group by   NB_YEAR_MFC_VEH,CD_CL_FUEL_ENG


select mel_region, CD_CL_FUEL_ENG, count(*) from `vic_ds`.`whole_fleet_vehicle_registration_snapshot_by_postcode_q_2_2024_2`
where  CD_CL_FUEL_ENG in ('E ','D ','P ') and mel_region!='null'
group by  mel_region, CD_CL_FUEL_ENG


select NB_YEAR_MFC_VEH, sum(total) from `vic_ds`.`monthly_new_vehicle_registration_july_2024` group by NB_YEAR_MFC_VEH;


select surveyperiod, sum(totalvehs) from vic_ds.households_vista_1220_lga
group by surveyperiod




