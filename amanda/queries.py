QUERIES = {
    "applications_received": """
    SELECT
        Foldertype,
        subcode,
        TO_CHAR(ROUND(INDATE, 'DDD'), 'YYYY-MM-DD'),
        COUNT(1) IssuedROWPermits
    FROM
        folder
    WHERE (foldertype in('DS')
        AND STATUSCODE NOT IN(50005, 50003, 70045)
        AND INDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
        AND INDATE IS NOT NULL)
        OR(foldertype in('RW', 'EX')
            AND STATUSCODE NOT IN(70045, 50003)
            AND INDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
            AND SUBCODE NOT IN(50510, 50505)
            AND INDATE IS NOT NULL)
    GROUP BY
        TO_CHAR(ROUND(INDATE, 'DDD'), 'YYYY-MM-DD'),
        Foldertype,
        subcode
    ORDER BY
        Foldertype
    """,
    "active_permits": """
    SELECT
        Foldertype,
        COUNT(1) ACTIVEPERMITS
    FROM
        folder
    WHERE (foldertype in('EX', 'DS')
        AND STATUSCODE IN(50010))
        OR(foldertype in('RW')
            AND STATUSCODE IN(50010)
            AND FOLDERNAME NOT LIKE 'LA-%')
    GROUP BY
        Foldertype
    ORDER BY
        Foldertype
    """,
    "issued_permits": """
    SELECT
        Foldertype,
        subcode,
        TO_CHAR(ROUND(ISSUEDATE, 'DDD'), 'YYYY-MM-DD'),
        COUNT(1) IssuedROWPermits
    FROM
        folder
    WHERE (foldertype in('EX', 'DS')
        AND ISSUEDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
        AND ISSUEDATE IS NOT NULL)
        OR(foldertype in('RW')
            AND ISSUEDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
            AND SUBCODE NOT IN(50510, 50505)
            AND ISSUEDATE IS NOT NULL)
    GROUP BY
        TO_CHAR(ROUND(ISSUEDATE, 'DDD'), 'YYYY-MM-DD'),
        Foldertype,
        subcode
    ORDER BY
        Foldertype
    """,
    "review_time": """
    WITH
        pa_first AS (
            SELECT
                fp.FOLDERRSN,
                fp.STARTDATE,
                fp.ENDDATE,
                row_number() over (
                    PARTITION BY
                        fp.FOLDERRSN
                    ORDER BY
                        fp.STARTDATE
                ) AS rn
            FROM
                FOLDERPROCESS fp
            WHERE
                fp.PROCESSCODE = 70000
        )
    SELECT
        f.CUSTOMFOLDERNUMBER,
        f.FOLDERRSN,
        f.FOLDERTYPE,
        TO_CHAR(f.INDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as INDATE,
        TO_CHAR(f.ISSUEDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as ISSUEDATE,
        TO_CHAR(pa.STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS WEBAPPSTART,
        TO_CHAR(pa.ENDDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS WEBAPPEND,
        f.issuedate - f.indate AS TIME_TO_ISSUANCE,
        pa.STARTDATE - f.indate AS TIME_TO_REVIEW
    FROM
        FOLDER f
        LEFT JOIN pa_first pa ON f.FOLDERRSN = pa.FOLDERRSN
        AND pa.rn = 1
    WHERE
        (
            (
                f.FOLDERTYPE = 'RW'
                AND f.SUBCODE = 50500
            )
            OR f.FOLDERTYPE = 'EX'
        )
        AND f.INDATE >= to_date('10-01-2022', 'mm-dd-yyyy')
        AND f.ISSUEDATE IS NOT NULL
    """,
    "ex_permits_issued": """
    SELECT
        CONCAT(CONCAT(f.FOLDERYEAR, '-'), f.FOLDERSEQUENCE) AS PERMIT_ID,
        f.SUBCODE,
        vs.SUBDESC,
        f.FOLDERNAME,
        TO_CHAR(f.INDATE,'MM-DD-YYYY HH24:MI:SS'),
        TO_CHAR(f.ISSUEDATE,'MM-DD-YYYY HH24:MI:SS')
    FROM
        FOLDER f
        LEFT OUTER JOIN VALIDSUB vs ON f.SUBCODE = vs.SUBCODE
    WHERE
        FOLDERTYPE in('EX')
        AND ISSUEDATE >= TO_DATE('10-01-2018', 'mm-dd-yyyy')
        AND ISSUEDATE IS NOT NULL
        AND PRIORITY = 3
	""",
    "license_agreements_timeline": """
    SELECT
        f.FOLDERRSN,
        f.REFERENCEFILE,
        sub.SUBDESC,
        vs.STATUSDESC,
        f.FOLDERCONDITION,
        vw.WORKDESC,
        TO_CHAR(f.INDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS INDATE,
        TO_CHAR(web_acceptance.ATTDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS WEB_APP_ACCEPT_DATE,
        TO_CHAR(payment.ATTDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS PAYMENT_COMPLETED_DATE,
        TO_CHAR(reviews.enddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS REVIEW_END_DATE,
        TO_CHAR(f.ISSUEDATE, 'YYYY-MM-DD"T"HH24:MI:SS') AS ISSUEDATE
    FROM
        FOLDER f
        LEFT OUTER JOIN (
        SELECT
            fp.FOLDERRSN,
            min(fpa.ATTEMPTDATE) AS ATTDATE -- Getting latest web acceptance
        FROM
            FOLDERPROCESS fp
            LEFT OUTER JOIN FOLDERPROCESSATTEMPT fpa ON fpa.PROCESSRSN = fp.PROCESSRSN
        WHERE
            fp.PROCESSCODE in(70000) -- Web Application acceptance process
            AND fpa.RESULTCODE in(52130) -- Only "Accepted" Attempts
        GROUP BY
            fp.FOLDERRSN) web_acceptance ON f.FOLDERRSN = web_acceptance.FOLDERRSN
        LEFT OUTER JOIN (
        SELECT
            fp.FOLDERRSN,
            min(fpa.ATTEMPTDATE) AS ATTDATE -- Getting latest completed distribution
        FROM
            FOLDERPROCESS fp
            LEFT OUTER JOIN FOLDERPROCESSATTEMPT fpa ON fpa.PROCESSRSN = fp.PROCESSRSN
        WHERE
            fp.PROCESSCODE in(51070) -- Initial Distribution process
            AND fpa.RESULTCODE in(55000) -- Only "Completed" Attempts
        GROUP BY
            fp.FOLDERRSN) payment ON f.FOLDERRSN = payment.FOLDERRSN
        LEFT OUTER JOIN (
        SELECT
            FOLDERRSN,
            max(ENDDATE) AS ENDDATE -- Getting most recent completed review
        FROM
            FOLDERPROCESS
        WHERE
            DISCIPLINECODE in(50030) -- Discipline group is "Review"
        GROUP BY
            FOLDERRSN) reviews ON f.FOLDERRSN = reviews.FOLDERRSN
        left outer JOIN VALIDSUB sub on sub.SUBCODE = f.SUBCODE
        left outer JOIN VALIDSTATUS vs on vs.STATUSCODE = f.statuscode
        left outer JOIN VALIDWORK vw on vw.WORKCODE = f.workcode
    WHERE
        f.FOLDERTYPE in('LM') -- Land Management folder type only
        AND f.STATUSCODE NOT in(56050) -- Remove VOID status
    """,
    "lde_site_plan_revisions": """
    SELECT
        f.FOLDERTYPE,
        f.FOLDERREVISION,
        f.FOLDERRSN,
        f.SUBCODE,
        sub.SUBDESC,
        vs.STATUSDESC,
        f.FOLDERCONDITION,
        f.REFERENCEFILE,
        f.FOLDERNAME,
        vu.USERNAME AS REVIEWER,
        fp.PROCESSRSN,
        vp.PROCESSDESC AS PROCESS_NAME,
        TO_CHAR(fp.STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as START_DATE,
        TO_CHAR(fp.ENDDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as END_DATE,
        TO_CHAR(fp.SCHEDULEDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as TO_START,
        TO_CHAR(fp.SCHEDULEENDDATE, 'YYYY-MM-DD"T"HH24:MI:SS') as TO_END,
        vps.STATUSDESC PROCESS_STATUS,
        ROW_NUMBER() OVER (PARTITION BY f.FOLDERRSN,
            fp.PROCESSCODE ORDER BY f.FOLDERRSN,
            fp.PROCESSCODE) cyclenumber,
        pi.PROPINFOVALUE CouncilDistrict
    FROM
        folder f
        JOIN folderprocess fp ON fp.FOLDERRSN = f.FOLDERRSN
            AND fp.PROCESSCODE IN(51212, 51258, 51259)
        JOIN validprocess vp ON vp.PROCESSCODE = fp.PROCESSCODE
        LEFT JOIN validuser vu ON vu.USERID = fp.ASSIGNEDUSER
        JOIN validprocessstatus vps ON vps.STATUSCODE = fp.STATUSCODE
        JOIN propertyinfo pi ON pi.PROPERTYRSN = f.PROPERTYRSN
            AND pi.PROPERTYINFOCODE = 52026 --Propertyinfo-Council District
        left outer JOIN VALIDSUB sub on sub.SUBCODE = f.SUBCODE
        left outer JOIN VALIDSTATUS vs on vs.STATUSCODE = f.statuscode
    GROUP BY
        f.FOLDERTYPE,
        f.FOLDERREVISION,
        vp.PROCESSDESC,
        f.FOLDERRSN,
        f.REFERENCEFILE,
        f.FOLDERNAME,
        fp.PROCESSCODE,
        fp.PROCESSRSN,
        vu.USERNAME,
        fp.SCHEDULEDATE,
        fp.SCHEDULEENDDATE,
        fp.STARTDATE,
        fp.ENDDATE,
        vps.STATUSDESC,
        pi.PROPINFOVALUE,
        f.SUBCODE,
        sub.SUBDESC,
        vs.STATUSDESC,
        f.FOLDERCONDITION
    ORDER BY
        vu.USERNAME,
        f.FOLDERTYPE,
        vp.PROCESSDESC,
        fp.PROCESSRSN
    """,
    "row_inspector_permit_list": """
    SELECT vs.subdesc                                      AS PERMIT_TYPE,
           f.foldertype                                    AS FOLDERTYPE,
           f.referencefile                                 AS PERMIT,
           f.folderrsn                                     AS FOLDERRSN,
           f.foldername                                    AS FOLDER_NAME,
           pr.propertyname                                 AS PROPERTY_NAME,
           f.expirydate                                    AS EXPIRY_DATE,
           f.issuedate                                     AS ISSUE_DATE,
           p.organizationname                              AS CONTRACTOR,
           p.phone1                                        AS PHONE,
           vw.workdesc                                     as RW_WORK_DESCRIPTION,
           trunc(trunc(f.expirydate) - trunc(f.issuedate)) AS WZ_Duration,          -- used for DS permits
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 75390)                        Total_Days,           -- only for RW permits
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 75980)                        Event_Start_Date,     -- only for RW permits
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 76110)                        Start_Date,           -- EX permits only
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 75993)                        Extension_Start_Date, -- EX permits only
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 75994)                        Extension_End_Date,   -- EX permits only
           (SELECT infovalue
            FROM folderinfo fi
            WHERE fi.folderrsn = f.folderrsn
              AND fi.infocode = 76115)                        End_Date,             -- EX permits only
           (SELECT count(processrsn) as count
            FROM folderprocessdeficiency fprd
            WHERE fprd.PROCESSRSN = fpr.PROCESSRSN
              AND fprd.statuscode = 55360)                    Count_deficiencies,
           (SELECT max(attemptdate) as inspection_date
            FROM AMANDA.FOLDERPROCESSATTEMPT fpra
            WHERE fpra.PROCESSRSN = fpr.PROCESSRSN)           Most_Recent_Inspection
    FROM folder f
             left JOIN validsub vs on vs.subcode = f.subcode
             left JOIN VALIDWORK vw on vw.workcode = f.workcode
             left JOIN property pr on pr.propertyrsn = f.propertyrsn
             left JOIN folderpeople fp on fp.folderrsn = f.folderrsn
             left JOIN people p on p.peoplersn = fp.peoplersn
             left join (select processrsn, folderrsn from FOLDERPROCESS where processcode = 50685) fpr
                       on fpr.FOLDERRSN = f.FOLDERRSN
    WHERE f.PRIORITY != 1
      AND (
        (f.foldertype = 'RW'
             AND f.subcode = 50500 --TURP only
             AND f.foldername NOT LIKE 'LA-%' -- removing LAs'
             AND fp.peoplecode = 1
             AND f.statuscode = 50010 --ACTIVE Permits only)
            OR (f.foldertype in ('EX', 'DS')
                AND fp.peoplecode = 50065 -- ROW Contractors
                AND f.statuscode = 50010))) --ACTIVE Permits only
    """,
    "row_inspector_segment_list": """
    SELECT
        fp.folderrsn AS FOLDERRSN,
        fp.propertyrsn AS PROPERTYRSN,
        CASE WHEN fp.PROPERTYRELATIONCODE = 5 THEN
            'TRUE'
        ELSE
            'FALSE'
        END AS is_primary
    FROM
        folder f,
        folderproperty fp,
        property p
    WHERE (f.foldertype = 'RW'
        AND f.subcode = 50500 --TURP
        AND f.statuscode = 50010 --ACTIVE
        AND f.foldername NOT LIKE 'LA-%'
        AND p.propcode = 52010
        AND f.folderrsn = fp.folderrsn
        AND fp.propertyrsn = p.propertyrsn)
        OR(f.foldertype in('EX', 'DS')
            AND f.statuscode = 50010 --ACTIVE
            AND p.propcode = 52010
            AND f.folderrsn = fp.folderrsn
            AND fp.propertyrsn = p.propertyrsn)
    ORDER BY
        fp.folderrsn
    """,
    "tds_cases": """
    SELECT
        foldertype,
        folderrsn,
        referencefile,
        foldername,
        Reviewer,
        processrsn,
        processname,
        tostart_date,
        duedate,
        started_date,
        ended_date,
        process_status,
        CAST(cyclenumber AS NUMBER(10, 0)) CycleNumber,
        CouncilDistrict
    FROM
        (
            SELECT
                f.foldertype,
                f.folderrsn,
                f.referencefile,
                f.foldername,
                vu.username Reviewer,
                fp.processrsn,
                vp.PROCESSDESC ProcessName,
                TO_CHAR(fp.scheduledate, 'YYYY-MM-DD"T"HH24:MI:SS') ToStart_Date,
                TO_CHAR(fp.scheduleEndDate, 'YYYY-MM-DD"T"HH24:MI:SS') DueDate,
                TO_CHAR(fp.startdate, 'YYYY-MM-DD"T"HH24:MI:SS') Started_Date,
                TO_CHAR(fp.enddate, 'YYYY-MM-DD"T"HH24:MI:SS') Ended_Date,
                vps.statusdesc process_status,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        f.folderrsn,
                        fp.processcode
                    ORDER BY
                        f.folderrsn,
                        fp.processcode
                ) cyclenumber,
                pi.propinfovalue CouncilDistrict
            FROM
                folder f
                JOIN folderprocess fp ON fp.folderrsn = f.folderrsn
                AND fp.processcode IN (51132, 51834, 84102)
                JOIN validprocess vp ON vp.processcode = fp.processcode
                LEFT JOIN validuser vu ON vu.userid = fp.assigneduser
                JOIN validprocessstatus vps ON vps.statuscode = fp.statuscode
                JOIN propertyinfo pi ON pi.propertyrsn = f.propertyrsn
                AND pi.propertyinfocode = 52026 --Propertyinfo-Council District
            WHERE
                f.foldertype IN ('C8', 'SP', 'ZC', 'PR', 'CC', 'DA', 'SC')
            GROUP BY
                f.foldertype,
                vp.PROCESSDESC,
                f.folderrsn,
                f.referencefile,
                f.foldername,
                fp.processcode,
                fp.processrsn,
                vu.username,
                fp.scheduledate,
                fp.scheduleEndDate,
                fp.startdate,
                fp.enddate,
                vps.statusdesc,
                pi.propinfovalue
            ORDER BY
                vu.username,
                f.foldertype,
                vp.processdesc,
                fp.processrsn
        )
    """,
    "tds_asmd_map": """
    SELECT
        f.folderrsn,
        f.parentrsn,
        p.PropHouse || ' ' || p.PropStreet || ' ' || p.PropStreetType || ' ' || p.PropStreetDirection || ' ' || p.PropUnitType || ' ' || p.PropUnit || ' ' || p.propprovince || ' ' || p.proppostal AS "Primary Folder Property Address",
        fp.propertyrsn AS "Primary Folder Property PROPID",
        p.propertyroll AS "Primary Folder Property Roll Number",
        PI.propinfovalue AS "Primary Folder Property SEGM_GIS_ID",
        vs.statusdesc,
        f.foldername,
        (
            SELECT
                sum(ab.totalpaid)
            FROM
                accountbill ab
            WHERE
                ab.folderrsn = f.folderrsn
        ) AS "Total amount fees Paid",
        FI1.infovalue AS "Offset Amount",
        FI2.infovalue AS "ROW Dedication Value Calculated",
        FI3.infovalue AS "Easement Dedication Value Calculated",
        (
            SELECT
                SUM(ff.F03)
            FROM
                folderfreeform ff
            WHERE
                ff.folderrsn = f.folderrsn
        ) AS "Total Affordability Units",
        FI4.infovalue AS "Council District",
        FI5.infovalue AS "SIF District Area",
        FI6.infovalue AS "Total Affordability Reduction",
        FI7.infovalue AS "Transportation Land Use"
    FROM
        folder f
        JOIN validstatus vs ON f.statuscode = vs.statuscode
        JOIN folderproperty fp ON f.folderrsn = fp.folderrsn
        AND f.propertyrsn = fp.propertyrsn
        JOIN property p ON p.propertyrsn = fp.propertyrsn
        JOIN propertyinfo PI ON p.propertyrsn = PI.propertyrsn
        AND PI.propertyinfocode = 55005
        JOIN FOLDERINFO FI1 ON F.FOLDERRSN = FI1.FOLDERRSN
        AND FI1.INFOCODE = 84021
        JOIN FOLDERINFO FI2 ON F.FOLDERRSN = FI2.FOLDERRSN
        AND FI2.INFOCODE = 84040
        JOIN FOLDERINFO FI3 ON F.FOLDERRSN = FI3.FOLDERRSN
        AND FI3.INFOCODE = 84043
        JOIN FOLDERINFO FI4 ON F.FOLDERRSN = FI4.FOLDERRSN
        AND FI4.INFOCODE = 84011
        JOIN FOLDERINFO FI5 ON F.FOLDERRSN = FI5.FOLDERRSN
        AND FI5.INFOCODE = 84012
        JOIN FOLDERINFO FI6 ON F.FOLDERRSN = FI6.FOLDERRSN
        AND FI6.INFOCODE = 84044
        JOIN FOLDERINFO FI7 ON F.FOLDERRSN = FI7.FOLDERRSN
        AND FI7.INFOCODE = 84016
    WHERE
        f.foldertype = 'SIF'
        AND f.statuscode != 70045
    ORDER BY
        f.folderrsn
    """,
    "sif_payment_details": """
    SELECT DISTINCT
        f.folderrsn,
        f.parentrsn,
        p.PropHouse || ' ' || p.PropStreet || ' ' || p.PropStreetType || ' ' || p.PropStreetDirection || ' ' || p.PropUnitType || ' ' || p.PropUnit || ' ' || p.propprovince || ' ' || p.proppostal AS primary_folder_property,
        fp.propertyrsn AS primary_folder_property_propid,
        p.propertyroll AS primary_folder_property_roll,
        pi.propinfovalue AS primary_folder_property_segm,
        FI1.infovalue AS council_district,
        FI2.infovalue AS sif_district_area,
        apd.paymentnumber AS paymentnumber,
        apd.paymentamount AS paymentamount,
        ab.billnumber AS billnumber,
        TO_CHAR(apd.dategenerated , 'YYYY-MM-DD"T"HH24:MI:SS') paymentdate,
        TO_CHAR(apd.dategenerated, 'Month') AS payment_month,
        CASE 
            WHEN EXTRACT(MONTH FROM apd.dategenerated) IN (10, 11, 12) THEN 'Q1'
            WHEN EXTRACT(MONTH FROM apd.dategenerated) IN (1, 2, 3) THEN 'Q2'
            WHEN EXTRACT(MONTH FROM apd.dategenerated) IN (4, 5, 6) THEN 'Q3'
            WHEN EXTRACT(MONTH FROM apd.dategenerated) IN (7, 8, 9) THEN 'Q4'
        END AS payment_quarter,
        CASE 
            WHEN EXTRACT(MONTH FROM apd.dategenerated) >= 10 
                THEN EXTRACT(YEAR FROM apd.dategenerated) + 1
            ELSE EXTRACT(YEAR FROM apd.dategenerated)
        END AS payment_fy
    FROM
        folder f
        JOIN folderfreeform ff ON f.folderrsn = ff.folderrsn
        JOIN accountbill ab ON f.folderrsn = ab.folderrsn
        JOIN Accountbillfee abf ON abf.folderrsn = ab.folderrsn
        AND abf.billnumber = ab.billnumber
        JOIN accountpaymentdetail apd ON apd.folderrsn = ab.folderrsn
        AND apd.billnumber = ab.Billnumber
        JOIN accountpayment ap ON apd.folderrsn = ap.folderrsn
        AND ap.paymentnumber = apd.paymentnumber
        AND ap.voidflag <> 'Y'
        JOIN validstatus vs ON f.statuscode = vs.statuscode
        JOIN folderproperty fp ON f.folderrsn = fp.folderrsn
        AND f.propertyrsn = fp.propertyrsn
        JOIN property p ON p.propertyrsn = fp.propertyrsn
        JOIN propertyinfo pi ON p.propertyrsn = pi.propertyrsn
        AND pi.propertyinfocode = 55005
        JOIN FOLDERINFO FI1 ON F.FOLDERRSN = FI1.FOLDERRSN
        AND FI1.INFOCODE = 84011
        JOIN FOLDERINFO FI2 ON F.FOLDERRSN = FI2.FOLDERRSN
        AND FI2.INFOCODE = 84012
    WHERE
        f.foldertype = 'SIF'
    ORDER BY
        f.folderrsn
    """,
    "active_contractors": """
    SELECT
        TRIM(folder.foldername) AS contractor_name,
        folder.foldercentury || folder.folderyear || '-' || folder.foldersequence || folder.foldertype AS license_number,
        TRUNC(folder.issuedate) AS issue_date,
        TRUNC(folder.expirydate) AS expiration_date,
        TRUNC(folderinfo.infovaluedatetime) AS insurance_expiration
    FROM
        folder,
        folderinfo
    WHERE
        (folder.foldertype = 'LC')
        AND (folder.issuedate IS NOT NULL)
        AND (folder.expirydate IS NOT NULL)
        AND FOLDER.STATUSCODE = 50010
        AND (
            folderinfo.folderrsn = folder.folderrsn
            AND folderinfo.infocode = 75350
        )
    ORDER BY
        contractor_name
    """,
    "license_agreements": """
    SELECT f.folderrsn,
       f.parentrsn,
       TO_CHAR(f.indate, 'YYYY-MM-DD"T"HH24:MI:SS') AS indate,
       TO_CHAR(f.issuedate, 'YYYY-MM-DD"T"HH24:MI:SS') AS issue_date,
       TO_CHAR(f.expirydate, 'YYYY-MM-DD"T"HH24:MI:SS') AS expiry_date,
       f.statuscode AS status_code,
       (SELECT MAX(vs.statusdesc)
        FROM validstatus vs
        WHERE vs.statuscode = f.statuscode) AS status_description,
       f.foldertype AS folder_type,
       f.subcode AS sub_code,
       (SELECT MAX(vst.subdesc)
        FROM validsub vst
        WHERE vst.subcode = f.subcode) AS subtype_description,
       f.workcode AS work_code,
       (SELECT vw.workdesc
        FROM validwork vw
        WHERE vw.workcode = f.workcode) AS worktype_description,
       f.foldername AS folder_name,
       f.referencefile AS reference_number,
       f.folderdescription AS description,
       (SELECT MAX(p.namefirst) || ' ' || MAX(p.namelast)
        FROM people p
               JOIN folderpeople fp ON p.peoplersn = fp.peoplersn
        WHERE fp.peoplecode = 1
          AND fp.folderrsn = f.folderrsn
          AND ROWNUM = 1) AS applicant_name,
       (SELECT MAX(Organizationname)
        FROM people p
               JOIN folderpeople fp ON p.peoplersn = fp.peoplersn
        WHERE fp.peoplecode = 1
          AND fp.folderrsn = f.folderrsn
          AND ROWNUM = 1) AS applicant_org_name,
       LISTAGG(
               fpr.propertyrsn || ',' || pr.PropertyName || ',' || pr.propertyroll || ',' || pr.propfloortype || ',' ||
               pr.propgisid1 || ',' || pr.propertyrsn || ',' || pr.propx || ',' || pr.propy || ',' || pr.legaldesc ||
               ',' || pr.propcountycode || ',' || pr.propcode,
               ' & ' ON OVERFLOW TRUNCATE '...'
       ) WITHIN GROUP (
                 ORDER BY
                   fpr.propertyrsn
                 ) AS property_details,
       CASE coa_folder.f_get_info_string(f.folderrsn, 50966)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS smart_housing_project,
       coa_folder.f_get_info_string(f.folderrsn, 51165) AS subdivision_name,
       coa_folder.f_get_info_string(f.folderrsn, 51168) AS subdivision_recording_number,
       coa_folder.f_get_info_string(f.folderrsn, 51511) AS sponsoring_department,
       CASE coa_folder.f_get_info_string(f.folderrsn, 51632)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS is_dedication_easement_in_row,
       CASE coa_folder.f_get_info_string(f.folderrsn, 51640)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS travis_county_row,
       CASE coa_folder.f_get_info_string(f.folderrsn, 51645)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS txdot_row,
       CASE coa_folder.f_get_info_string(f.folderrsn, 51650)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS in_coa_parkland,
       CASE coa_folder.f_get_info_string(f.folderrsn, 51665)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS existing_site_plan,
       coa_folder.f_get_info_string(f.folderrsn, 51671) AS existing_site_plan_case_not,
       CASE coa_folder.f_get_info_string(f.folderrsn, 51676)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS texas_walk_of_star_area,
       TO_CHAR(coa_folder.f_get_info_date(f.folderrsn, 57035), 'YYYY-MM-DD"T"HH24:MI:SS') AS pc_hearing_date,
       TO_CHAR(coa_folder.f_get_info_date(f.folderrsn, 57037), 'YYYY-MM-DD"T"HH24:MI:SS') AS utc_hearing_date,
       coa_folder.f_get_info_string(f.folderrsn, 57042) AS new_easement_dedication_required,
       TO_CHAR(coa_folder.f_get_info_date(f.folderrsn, 57044), 'YYYY-MM-DD"T"HH24:MI:SS') AS legal_review_date,
       coa_folder.f_get_info_string(f.folderrsn, 57045) AS new_easement_dedication_required,
       coa_folder.f_get_info_string(f.folderrsn, 61595) AS physical_address,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76366)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS license_agreement_termination,
       coa_folder.f_get_info_string(f.folderrsn, 76372) AS la_file_number_termination,
       coa_folder.f_get_info_string(f.folderrsn, 76374) AS la_file_number_amendment,
       coa_folder.f_get_info_string(f.folderrsn, 76376) AS appraisal_district_geoid_no,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76649)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS tower_crane,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76708)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS tie_back_retention,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76101)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS survey,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76102)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS improvement_in_easement,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76104)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS aulcc_required,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76113)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS landowner_approval,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76124)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS central_business_district_cbd,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76127)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS historical_land_commission_approved,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76128)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS owners_recorded_deed,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76136)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS improvements_to_be_installed_in_row,
       coa_folder.f_get_info_string(f.folderrsn, 76137) AS county_clerk_recordation_no,
       coa_folder.f_get_info_string(f.folderrsn, 76139) AS govt_non_coa_app_type,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76145)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS historical_zoning,
       CASE coa_folder.f_get_info_string(f.folderrsn, 76146)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS historic_zoning_district,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79759)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS subdivision_case,
       coa_folder.f_get_info_string(f.folderrsn, 79760) AS subdivision_case_no,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79761)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS zoning_case,
       coa_folder.f_get_info_string(f.folderrsn, 79762) AS purposeof_release_easement,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79764)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS existing_infrastructure_within_easement,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79765)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS is_property_subdivided,
       coa_folder.f_get_info_string(f.folderrsn, 79766) AS full_or_partial_releaseof_easement,
       coa_folder.f_get_info_string(f.folderrsn, 79767) AS size_of_easement_track_released,
       coa_folder.f_get_info_string(f.folderrsn, 79768) AS acres_or_sqft,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79769)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS was_easement_dedicated_plats,
       coa_folder.f_get_info_string(f.folderrsn, 79770) AS plat_recording_number_easement,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79771)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS was_separate_instrument,
       coa_folder.f_get_info_string(f.folderrsn, 79772) AS separate_instrument_recording_number,
       coa_folder.f_get_info_string(f.folderrsn, 79773) AS name_of_development_project,
       coa_folder.f_get_info_string(f.folderrsn, 79774) AS owner_or_entity_type,
       coa_folder.f_get_info_string(f.folderrsn, 79775) AS plan_for_area_to_be_vacated,
       coa_folder.f_get_info_string(f.folderrsn, 77935) AS zoning_case_number,
       coa_folder.f_get_info_string(f.folderrsn, 79220) AS site_plan_id,
       coa_folder.f_get_info_string(f.folderrsn, 79720) AS lm_request_type,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79723)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS is_area_vacated_public_row,
       coa_folder.f_get_info_string(f.folderrsn, 79725) AS purpose_of_vacation,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79726)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS is_project_unified_development,
       coa_folder.f_get_info_string(f.folderrsn, 79727) AS project_type,
       TO_CHAR(coa_folder.f_get_info_date(f.folderrsn, 79728),
               'YYYY-MM-DD"T"HH24:MI:SS') AS proposed_construction_start_date,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79730)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS adjacent_property_developed,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79731)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS utility_lines_to_be_vacated,
       coa_folder.f_get_info_string(f.folderrsn, 79735) AS existing_parking_facilities,
       coa_folder.f_get_info_string(f.folderrsn, 79741) AS ownership_deed_numbers,
       coa_folder.f_get_info_string(f.folderrsn, 79742) AS ownership_deed_dates,
       coa_folder.f_get_info_string(f.folderrsn, 79743) AS ownership_deed_county,
       coa_folder.f_get_info_string(f.folderrsn, 79745) AS row_dedication_recording_number,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79748)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS city_purchased_vacated_area,
       coa_folder.f_get_info_string(f.folderrsn, 79749) AS citys_deed_recording_number,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79751)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS area_vacated_functional_row,
       CASE coa_folder.f_get_info_string(f.folderrsn, 79752)
         WHEN 'Yes' THEN 'TRUE'
         WHEN 'No' THEN 'FALSE'
         ELSE NULL
         END AS area_vacated_on_paper_only
    FROM folder F
           LEFT JOIN folderproperty fpr ON f.folderrsn = fpr.folderrsn
           INNER JOIN property pr ON pr.propertyrsn = fpr.propertyrsn
           LEFT JOIN folderpeople fp ON f.folderrsn = fp.folderrsn
    WHERE f.foldertype = 'LM'
      AND f.statuscode NOT IN (70045, 70015, 51040, 50003)
    GROUP BY f.folderrsn,
             f.parentrsn,
             f.indate,
             f.issuedate,
             f.expirydate,
             f.statuscode,
             f.foldertype,
             f.subcode,
             f.workcode,
             f.foldername,
             f.referencefile,
             f.folderdescription
    """,
}
