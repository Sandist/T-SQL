create or alter procedure kmm.fillTable#ALLOC#TEAM_TO_PPM_ENTITY @deletedEntriesCounter numeric(18) = null out,
                                                                    @updatedEntriesCounter numeric(18) = null out,
                                                                    @insertedEntriesCounter numeric(18) = null out
as
begin
    set nocount on;
    set ansi_nulls on;
    set quoted_identifier on;
    set @deletedEntriesCounter = 0; set @updatedEntriesCounter = 0; set @insertedEntriesCounter = 0;
    declare @startTimestamp$UTC datetime2 = sysutcdatetime(), @procName sysname = concat(object_schema_name(@@procid), '.', object_name(@@procid));

    begin try
        declare @statistics table
                            (
                                flag int
                            );

        begin transaction;

		with team2ppme as (select ji.ID                                                                        as TEAM_ID
								, JR_S2200.getIssueKey(ji.ID)                                                  as TEAM_KEY
								, cast(ji.CREATED_AT as date)                                                  as TEAM_CARD$CREATED_AT
								, JR_S2200.getCFSingleStringValue(JR_S2200.cfidPPM_ENTITY_KEY$4TEAMS(), ji.ID) as PPM_ENTITY_KEY$RAW
						   from JR_S2200.RAW#ISSUES as ji
						   where ji.PROJECT_ID in (JR_S2200.pidTEAMS@IT(), JR_S2200.pidTEAMS@BUSINESS()))

		   , history$lcpd as (select top 1 with ties chg.ID                                                  as CHANGE_ID
												   , cast(chg.TIMESTAMP as date)                             as CHANGE_TIMESTAMP
												   , chg.ISSUE_ID                                            as TEAM_ID
												   , cast(isnull(NEW_VALUE, NEW_DESCRIPTION) as varchar(32)) as PPM_ENTITY_KEY$RAW
							  from JR_S2200.RAW#ISSUE_CHANGELOG_GROUPS as chg
									   inner join JR_S2200.RAW#ISSUE_CHANGELOG_ITEMS as chi
												  on chi.GROUP_ID = chg.ID
							  where chg.ISSUE_ID in (select TEAM_ID from team2ppme)
								and chi.FIELD_TYPE_KEY = JR.iftCUSTOM()
								and chi.FIELD_NAME$RAW in (	select convert(nvarchar(510), NAME collate Cyrillic_General_CI_AS) as NAME
															from JR_S2200.SYS#CUSTOM_FIELD_NAMES as cfn
															where ID = JR_S2200.cfidPPM_ENTITY_KEY$4TEAMS())
								and ((OLD_VALUE is null and OLD_DESCRIPTION is null)
								  or (NEW_VALUE is null and NEW_DESCRIPTION is null)
								  or cast(isnull(OLD_VALUE, OLD_DESCRIPTION) as varchar(32)) != cast(isnull(NEW_VALUE, NEW_DESCRIPTION) as varchar(32)))
							  order by row_number() over (
								  partition by chg.ISSUE_ID, cast(chg.TIMESTAMP as date)
								  order by chg.TIMESTAMP desc, chi.ID desc))

		   , history as (select *
							  , lag(PPM_ENTITY_KEY$RAW)
									over (partition by TEAM_ID order by CHANGE_TIMESTAMP) PPM_ENTITY_KEY$RAW#PREV
						 from history$lcpd)

		   , pre_result as (select team2ppme.TEAM_ID                                                      as TEAM_ID
								 , team2ppme.TEAM_KEY                                                     as TEAM_KEY
								 , iif(trim(replace(rkey.raw_rittm_key, '0', '')) = '',
									   JR.iidPPM_ENTITY#ZERO(),
									   JR_S2200.resolveIssueIdByKey(rkey.raw_rittm_key))                  as PPM_ENTITY_ID
								 , iif(history.CHANGE_ID is null,
									   team2ppme.TEAM_CARD$CREATED_AT, history.CHANGE_TIMESTAMP)          as DATE_FROM
								 , iif(history.CHANGE_ID is null,
									   datefromparts(year(sysdatetime()), 12, 31),
									   lead(history.CHANGE_TIMESTAMP, 1,
											datefromparts(year(sysdatetime()), 12, 31)) over (
												partition by history.TEAM_ID order by history.CHANGE_ID)) as DATE_TO
							from team2ppme
									 left join history
											   on team2ppme.TEAM_ID = history.TEAM_ID
												   and isnull(history.PPM_ENTITY_KEY$RAW, '') <> isnull(history.PPM_ENTITY_KEY$RAW#PREV, '')
									 outer apply (select iif(history.CHANGE_ID is null,
															 team2ppme.PPM_ENTITY_KEY$RAW,
															 history.PPM_ENTITY_KEY$RAW) as raw_rittm_key) rkey)
			, result as (select convert(numeric(18), TEAM_ID)					as TEAM_ID
							  , convert(varchar(32), MART.a.trim(TEAM_KEY)
									collate Cyrillic_General_100_CI_AS_SC_UTF8) as TEAM_KEY
							  , convert(numeric(18), PPM_ENTITY_ID)				as PPM_ENTITY_ID
							  , convert(varchar(32), MART.a.trim(
								 iif(PPM_ENTITY_ID = JR.iidPPM_ENTITY#ZERO()
									 , JR.ikeyPPM_ENTITY#ZERO()
									 , JR_S2200.getIssueKey(PPM_ENTITY_ID)))
									collate Cyrillic_General_100_CI_AS_SC_UTF8)	as PPM_ENTITY_KEY
							  , convert(date, DATE_FROM)						as DATE_FROM
							  , convert(date, max(DATE_TO))						as DATE_TO
						 from pre_result
						 where PPM_ENTITY_ID is not null
						 group by TEAM_ID, TEAM_KEY, PPM_ENTITY_ID, DATE_FROM)
		merge kmm.ALLOC#TEAM_TO_PPM_ENTITY with (holdlock) as tgt
        using (select *,
                      hashbytes('SHA2_256'
                          , concat_ws('|', TEAM_KEY, PPM_ENTITY_KEY, DATE_TO)) as #hash
               from result) as src
        on src.TEAM_ID = tgt.TEAM_ID
		and src.PPM_ENTITY_ID = tgt.PPM_ENTITY_ID
		and src.DATE_FROM = tgt.DATE_FROM
        when matched and (src.#hash != tgt.#hash or (tgt.#flags & UKITR.flgDES$DELETED()) != 0)
            then
            update
            set tgt.TEAM_KEY        = MART.a.trim(src.TEAM_KEY)
              , tgt.PPM_ENTITY_KEY  = MART.a.trim(src.PPM_ENTITY_KEY)
              , tgt.DATE_TO			= src.DATE_TO
			  , tgt.#timestamp		= default
              , tgt.#flags		   -= (tgt.#flags & UKITR.flgDES$DELETED())
              , tgt.#hash			= src.#hash
        when not matched
            then
            insert (TEAM_ID, TEAM_KEY, PPM_ENTITY_ID, PPM_ENTITY_KEY, DATE_FROM, DATE_TO, #flags, #hash)
            values ( src.TEAM_ID, MART.a.trim(src.TEAM_KEY)
                   , src.PPM_ENTITY_ID, MART.a.trim(src.PPM_ENTITY_KEY)
                   , src.DATE_FROM, src.DATE_TO
                   , 0, src.#hash)
        when not matched by source and (tgt.#flags & UKITR.flgDES$DELETED()) = 0
            then
            update
            set tgt.#timestamp = default
			  , tgt.#flags	  += UKITR.flgDES$DELETED()
            output case $action
                       when 'INSERT' then 1
                       when 'UPDATE' then
                           iif((inserted.#flags & UKITR.flgDES$DELETED()) != 0
                                   and (deleted.#flags & UKITR.flgDES$DELETED()) = 0, -1, 0) end into @statistics;
        commit transaction;
    end try
    begin catch
        if @@trancount > 0 rollback transaction;
        throw;
    end catch

    set @deletedEntriesCounter = (select count(*) from @statistics where flag = -1);
    set @updatedEntriesCounter = (select count(*) from @statistics where flag = 0);
    set @insertedEntriesCounter = (select count(*) from @statistics where flag = 1);

    print concat(char(13), char(10), @procName, ' STATISTICS: '
        , @deletedEntriesCounter, ' ROWS MARKED AS DELETED, ', @updatedEntriesCounter, ' ROWS UPDATED, ', @insertedEntriesCounter, ' ROWS INSERTED'
        , ' (execution time - ', datediff(second, @startTimestamp$UTC, sysutcdatetime()), ' seconds)', char(13), char(10));
end