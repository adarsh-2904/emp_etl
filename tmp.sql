UPDATE curated.scd2_dim_employee AS target
                SET effective_end_date = NOW(),
                    is_current = 0
                FROM curated.emp_stg AS source
                WHERE target.employee_id = source.nk_id
                AND target.is_current = 1
                AND (
                    target.education IS DISTINCT FROM source.education OR
                    target.city IS DISTINCT FROM source.city OR
                    target.paymenttier IS DISTINCT FROM source.paymenttier OR
                    target.age IS DISTINCT FROM source.age OR
                    target.gender IS DISTINCT FROM source.gender OR
                    target.everbenched IS DISTINCT FROM source.everbenched OR
                    target.experienceincurrentdomain IS DISTINCT FROM source.experienceincurrentdomain OR
                    target.leaveornot IS DISTINCT FROM source.leaveornot
                );