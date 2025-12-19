package taskmanager.jdbc;

public interface RequestsInTasks {

    /* 1. EqTaskProvider.kt */
    String eq = """
            select id, employee_id
            from surms.employee_queue
            where not processed
                and not is_locked
                and (check_after <= current_timestamp or check_after is null)
            order by queue_priority, id
            """;

    /* 2. EsqSudirTaskProvider.kt */
    String esqSudir = """
            SELECT ESQ.id as ID,
                   ESQ.employee_queue_id as EQ_ID
            FROM surms.employee_service_queue ESQ
            WHERE ESQ.processed = false
              AND ESQ.check_after <= current_timestamp
              AND ESQ.data is not null
              AND ESQ.result_status_id is not null
              AND ESQ.target = 'SUDIR'
            ORDER BY esq.id
            """;

    /* 3. ApplyNotificationTaskProvider.kt */
    String applyNotification = """
                    with done_status as (select id from queue_status where NAME = 'DONE'),
                 eq as (select *
                        from employee_queue
                        where processed
                          and not message_processed
                          and result_status_id = (select id from queue_status where NAME = 'DONE')),
                 eq_min as (select min(id) id from eq)
            select temp.EQ_ID, temp.EMPLOYEE_ID
            from (
                     select eq.id                                               as EQ_ID,
                            eq.employee_id                                      as EMPLOYEE_ID,
                            sum(case
                                    when not esq.processed and
                                         (result_comment not like '%в процессе обработки%' or result_comment is null) then 1
                                    else 0 end)                                 as esqs,
                            sum(case when not smq.processed then 1 else 0 end)  as smqs,
                            sum(case when not sudq.processed then 1 else 0 end) as sudqs
                     from eq
                              join done_status on eq.result_status_id = done_status.id
                              join employee_service_queue esq on eq.id = esq.employee_queue_id
                              join eq_min on esq.employee_queue_id >= eq_min.id
                              left join sm_queue smq on ESQ.id = smq.employee_service_queue_id
                              left join sudir_queue sudq on ESQ.ID = sudq.employee_service_queue_id
                     group by eq.id, eq.employee_id) temp
            where esqs = 0
              and smqs = 0
              """;

    /* 4. EsqEsrtTaskProvider.kt */
    String esqEsrt = """
            select esq.id as ID, esq.employee_queue_id as EQ_ID
            from surms.employee_service_queue esq
            join employee_queue eq on esq.employee_queue_id = eq.id 
            join source_label sl on eq.source_id = sl.id 
            where not esq.processed
                and esq.check_after <= current_timestamp
                and esq.target = 'SM_ESRT'
            order by sl.pq_value, esq.result_comment nulls first, esq.id
            limit 1500
            """;
    /* 5. PostApplyTaskProvider.kt */
    String postApply = """
            select id, employee_id
            from employee_post_apply_queue
            where not processed
                and post_apply_date <= current_timestamp
            order by id
            """;

    /* 6. CreateZNOTaskRepository.kt */
    String createZno = """
                SELECT id,
                contact_name, callback_contact, category, title, description, service,
                tpl_id, priority, assignment_group, assignment_name,
                approval_strategy_1, approvers_1, approval_strategy_2, approvers_2,
                approval_strategy_3, approvers_3, ci
                FROM surms.sm_queue_agreement
                WHERE processed = false
            """;

    /* 7. CreateSDTaskRepository.kt */
    String createSd = """
                select sq.id,
                       sq.contact_name,
                       sq.callback_contact,
                       sq.affected_item_name,
                       sq.logical_name,
                       sq.initial_impact,
                       sq.impact,
                       sq.category,
                       sq.work_type,
                       sq.assignment_name,
                       sq.title,
                       sq.description,
                       sq.parameters,
                       sq.tpl_id,
                       sq.process_impact,
                       sq.id_profile,
                       sq.id_rights,
                       sq.appr_type
                from surms.sm_queue sq
                join employee_service_queue esq on sq.employee_service_queue_id  = esq.id 
                join employee_queue eq on esq.employee_queue_id = eq.id 
                join source_label sl on eq.source_id = sl.id 
                where not sq.processed
                order by case when esq.target = 'SM' then 1 when esq.target = 'SM_ESRT' then 2 else 3 end, sl.pq_value, sq.id
                limit 150
            """;

    /* 8. CompleteZNOTaskRepository.kt */
    String completeZno = """
                SELECT id, object_id, code, description FROM surms.sm_queue_action
                WHERE processed = false AND action = 'Complete' AND object_type ='Agreement'
            """;

    /* 9. DenyZNOTaskRepository.kt */
    String denyZno = """
                SELECT id , object_id, code, description
                FROM surms.sm_queue_action
                WHERE processed = false AND action = 'CallBack' AND object_type='Agreement'
            """;

    /* 10. CompleteEsqSmTaskRepository.kt */
    String completeEsqSm = """
            select esq.id as ID, esq.employee_queue_id as EQ_ID
            from surms.employee_service_queue esq
            join employee_queue eq on esq.employee_queue_id = eq.id 
            join source_label sl on eq.source_id = sl.id 
            where not esq.processed
                and esq.check_after <= current_timestamp
                and esq.target = 'SM'
            order by sl.pq_value, esq.result_comment nulls first, esq.id
            limit 3000
            """;

    /* 11. EsqAdTaskProvider.kt */
    String esqAd = """
            SELECT esq.id as esqId, 
                esq.base_service_id as serviceId, 
                esq.data as data, 
                qa.name as action
            FROM employee_service_queue esq
            JOIN queue_action qa on qa.id = esq.action_id
            JOIN queue_status qs on qs.id = esq.result_status_id
            WHERE esq.processed = false
              AND esq.check_after <= current_timestamp
              AND esq.target = 'AD'
              AND qs.name != 'WAITING' 
            ORDER BY esq.id
            """;

    /* 12. EsqIdmTaskProvider.kt */
    String esqIdm = """
                    select esq.employee_queue_id                                       AS eq_id
                          , eq.employee_id                                             AS employee_id
                          , esq.id                                                     AS esq_id
                          , e.tn                                                       AS employee_tn
                          , concat(e.last_name, ' ', e.first_name, ' ', e.middle_name) AS employee_full_name
                          , eq.apply_summary                                           AS apply_summary
                          , qa.name                                                    AS action
                          , esq.data                                                   AS esq_data
                    from employee_service_queue esq
                        join employee_queue eq  ON eq.id = esq.employee_queue_id
                        join employee e  ON e.id = eq.employee_id
                        join base_service bs ON bs.id = esq.base_service_id
                        join queue_action qa ON qa.id = esq.action_id
                    where esq.data is not null
                        and esq.processed = false
                        and esq.check_after <= current_timestamp
                        and esq.result_status_id is not null
                      and esq.target = 'IDM'
            """;
}

