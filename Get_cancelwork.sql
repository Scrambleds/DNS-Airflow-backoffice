--Query ที่ 1

SELECT
S.SALEID,
xininsure.getbookname(s.periodid,
s.salebookcode,
s.sequence) AS bookname,
s.salebookcode,
s.routeid ,
B.region,
r.routecode,
s.paidamount,
s.cancelresultid,
(
SELECT
SB.BYTECODE
FROM
XININSURE.SYSBYTEDES SB
WHERE
SB.COLUMNNAME = 'PAYMENTSTATUS'
AND SB.TABLENAME = 'SALE'
AND SB.BYTECODE = S.PAYMENTSTATUS) AS PAYMENTSTATUS,
S.PAYMENTMODE,
S.PRBPOLICYNUMBER,
F.STAFFCODE,
F.STAFFTYPE,
F.STAFFCODE || ':' || F.STAFFNAME AS STAFFNAME,
D.DEPARTMENTID,
D.DEPARTMENTCODE || ':' || D.DEPARTMENTNAME AS DEPARTMENTNAME,
D.DEPARTMENTCODE,
D.DEPARTMENTGROUP,
ssa.actionid,
ssa.actioncode,
ssa.actionstatus,
(
SELECT
r.PROVINCECODE
FROM
XININSURE.ROUTE r
WHERE
s.ROUTEID = r.ROUTEID) AS PROVINCECODE,
ssa.RESULTID,
(
SELECT
r.RESULTCODE
FROM
XININSURE.RESULT r
WHERE
r.RESULTID = ssa.RESULTID) AS RESULTCODE,
S.MASTERSALEID,
ssa.duedate,
(SELECT MAX(r.SAVEDATE )
FROM XININSURE.RECEIVEITEMCLEAR T ,
xininsure.receiveitem i,
xininsure.receive r
WHERE t.receiveid = i.receiveid
and t.receiveitem = i.receiveitem
and i.receiveid = r.receiveid
and r.receivestatus in ('S','C')
and i.receivebookcode not in('R01','R03','R02','R04')
and T.SALEID = SSA.SALEID ) AS LASTUPDATEDATETIME,
(
SELECT
max(rc.installment)
-- ชำระล่าสุดงวดที่
FROM
xininsure.receiveitem ri,
xininsure.receiveitemclear rc
WHERE
ri.receiveid = rc.receiveid
AND rc.receiveitem = rc.receiveitem
AND ri.saleid = S.SALEID) AS LASTINSTALLMENT,
(
SELECT
max(sa.installment)
--- งานที่ตามอยู่
FROM
xininsure.saleaction sa
WHERE
sa.actionid = ssa.actionid
AND sa.actionstatus = 'W'
AND sa.saleid = S.SALEID
AND sa.sequence = (
SELECT
max(sa.sequence)
FROM
xininsure.saleaction sa
WHERE
sa.actionid = ssa.actionid
AND sa.actionstatus = 'W'
AND sa.saleid = S.SALEID)) AS FOLLOWINSTALLMENT,
su.SUPPLIERCODE,
NVL(
(
SELECT
s.BALANCE
FROM
xininsure.salepayment s
WHERE
s.saleid = ssa.saleid
AND s.balance > 0
AND s.installment = (
SELECT
sa.installment
FROM
xininsure.saleaction sa
WHERE
sa.saleid = ssa.saleid
AND sa.actionid = 44
-- C01
AND sa.actionstatus = 'W'
AND sa.sequence = (
SELECT
max(sa.sequence)
FROM
xininsure.saleaction sa
WHERE
sa.saleid = ssa.saleid
AND sa.actionid = 44
-- C01
AND sa.actionstatus = 'W'
)
)
),
0
)AS balance,
PT.PRODUCTGROUP,
PT.PRODUCTTYPE
FROM
XININSURE.SALE S,
XININSURE.STAFF F,
XININSURE.DEPARTMENT D,
XININSURE.PRODUCT P,
XININSURE.PRODUCTTYPE PT,
XININSURE.SUPPLIER SU,
XININSURE.ROUTE R,
XININSURE.BRANCH B,
(
SELECT
SA.SALEID ,
SA.INSTALLMENT,
a.actionid,
a.actioncode ,
sa.actionstatus,
sa.RESULTID,
sa.duedate,
sa.sequence
FROM
XININSURE.SALEACTION SA,
xininsure.action a
WHERE
SA.ACTIONID IN(3760, 3761, 2261, 3740, 3741, 3742, 3743,
5933, 11293, 7533, 9133, 9174, 11574, 11575, 11576,
11577, 11553, 11554, 11555, 15014)
AND sa.actionid = a.actionid
--ดำเนินการแล้ว
AND SA.ACTIONSTATUS IN ('R', 'W', 'Y')
AND SA.DUEDATE = TO_DATE('30/07/2024', 'DD/MM/YYYY')
-- AND SA.DUEDATE = TRUNC(SYSDATE)
-- AND SA.DUEDATE >= TO_DATE('25/07/2024', 'DD/MM/YYYY')
-- AND SA.DUEDATE <= TO_DATE('30/07/2024', 'DD/MM/YYYY' ) --AND SA.DUEDATE <=TRUNC(SYSDATE) - 1 --AND sa.duedate>=
    to_date('07/05/2025', 'dd/mm/yyyy')
    --AND sa.duedate <= to_date('10/05/2020', 'dd/mm/yyyy' ) ) SSA WHERE S.SALEID=SSA.SALEID AND S.STAFFID=F.STAFFID AND
        S.STAFFDEPARTMENTID=D.DEPARTMENTID AND S.ROUTEID=R.ROUTEID AND S.PRODUCTID=P.PRODUCTID AND
        P.SUPPLIERID=SU.SUPPLIERID AND P.PRODUCTTYPE=PT.PRODUCTTYPE AND B.BRANCHID=R.BRANCHID -- AND
        PT.PRODUCTGROUP='MT' AND SU.SUPPLIERCODE IN
        ('KWIL','AIA','SSL','BKI','BLA','VY','DHP','TVI','MTI','MTL','MLI','ALIFE','FWD','KTAL','ACE','SELIC','PLA','TSLI', 'ESY'
        ) -- AND SU.SUPPLIERCODE IN ('ESY') --อนุมัติ AND S.PLATEID IS NULL AND S.CANCELDATE IS NULL AND
        S.CANCELEFFECTIVEDATE IS NULL -- AND S.SALEID=43415566 -- AND PT.PRODUCTTYPE IN ('LOAN', 'LOANX' ) -- FETCH
        FIRST 1 ROWS only ORDER BY s.SALEID DESC SELECT PRBPOLICYNUMBER FROM XININSURE.SALE



--Query ที่ 2

SELECT 
    S.SALEID,
    XININSURE.GETBOOKNAME(S.PERIODID, S.SALEBOOKCODE, S.SEQUENCE) AS SALEORDER,
    S.SALEBOOKCODE,
    S.ROUTEID,
    B.REGION,
    R.ROUTECODE,
    SU.SUPPLIERCODE AS บริษัท,
    S.POLICYDATE,
    S.SALESTATUS,
    S.POLICYNUMBER,
    S.PRBPOLICYNUMBER,
    S.INSUREPREFIX || S.INSURENAME || ' ' || S.INSURESURNAME AS ชื่อลูกค้า,
    (SELECT A.ACTIONCODE
     FROM XININSURE.SALEACTION SA, XININSURE.ACTION A
     WHERE SA.SALEID = S.SALEID
       AND SA.ACTIONID = A.ACTIONID
       AND SA.ACTIONSTATUS IN ('R', 'W', 'Y')
       AND SA.SEQUENCE = (SELECT MAX(SA2.SEQUENCE)
                          FROM XININSURE.SALEACTION SA2, XININSURE.ACTION A2
                          WHERE SA2.SALEID = S.SALEID
                            AND SA2.ACTIONID = A2.ACTIONID
                            AND SA2.ACTIONSTATUS = 'Y')
    ) AS งาน,
    F.STAFFCODE,
    F.STAFFTYPE,
    F.STAFFCODE || ':' || F.STAFFNAME AS พนักงาน,
    D.DEPARTMENTID,
    D.DEPARTMENTCODE,
    D.DEPARTMENTGROUP,
    D.DEPARTMENTCODE || ':' || D.DEPARTMENTNAME AS ทีม,
    SSA.ACTIONID,
    SSA.ACTIONCODE,
    SSA.ACTIONSTATUS,
    (SELECT r.PROVINCECODE
     FROM XININSURE.ROUTE r
     WHERE s.ROUTEID = r.ROUTEID) AS PROVINCECODE,
    SSA.RESULTID,
    (SELECT r.RESULTCODE
     FROM XININSURE.RESULT r
     WHERE r.RESULTID = ssa.RESULTID) AS RESULTCODE,
    (SELECT A.RESULTCODE ||':'|| A.RESULTNAME
     FROM XININSURE.SALEACTION SA, XININSURE.RESULT A
     WHERE SA.SALEID = S.SALEID
       AND SA.RESULTID = A.RESULTID
       AND SA.ACTIONSTATUS IN ('R', 'W', 'Y')
       AND SA.SEQUENCE = (SELECT MAX(SA2.SEQUENCE)
                          FROM XININSURE.SALEACTION SA2, XININSURE.ACTION A2
                          WHERE SA2.SALEID = S.SALEID
                            AND SA2.ACTIONID = A2.ACTIONID
                            AND SA2.ACTIONSTATUS = 'Y')
    ) AS รหัสผล,
    (SELECT SA.ACTIONREMARK
     FROM XININSURE.SALEACTION SA 
     WHERE SA.SALEID = S.SALEID
       AND SA.ACTIONSTATUS IN ('R', 'W', 'Y')
       AND SA.SEQUENCE = (SELECT MAX(SA2.SEQUENCE)
                          FROM XININSURE.SALEACTION SA2, XININSURE.ACTION A2
                          WHERE SA2.SALEID = S.SALEID
                            AND SA2.ACTIONID = A2.ACTIONID
                            AND SA2.ACTIONSTATUS = 'Y')
    ) AS เหตุผลการตอบ,
    (SELECT SA.REQUESTREMARK
     FROM XININSURE.SALEACTION SA 
     WHERE SA.SALEID = S.SALEID
       AND SA.ACTIONSTATUS IN ('R', 'W', 'Y')
       AND SA.SEQUENCE = (SELECT MAX(SA2.SEQUENCE)
                          FROM XININSURE.SALEACTION SA2, XININSURE.ACTION A2
                          WHERE SA2.SALEID = S.SALEID
                            AND SA2.ACTIONID = A2.ACTIONID
                            AND SA2.ACTIONSTATUS = 'Y')
    ) AS หมายเหตุการแจ้ง,
    S.MASTERSALEID,
    SSA.DUEDATE,
    (SELECT MAX(r.SAVEDATE) 
     FROM XININSURE.RECEIVEITEMCLEAR T, 
          XININSURE.RECEIVEITEM i,
          XININSURE.RECEIVE r              
     WHERE t.receiveid = i.receiveid
       AND t.receiveitem = i.receiveitem
       AND i.receiveid = r.receiveid
       AND r.receivestatus IN ('S','C')
       AND i.receivebookcode NOT IN('R01','R03','R02','R04')
       AND T.SALEID = SSA.SALEID) AS LASTUPDATEDATETIME,
    (SELECT MAX(rc.installment)
     FROM XININSURE.RECEIVEITEM ri,
          XININSURE.RECEIVEITEMCLEAR rc
     WHERE ri.receiveid = rc.receiveid
       AND rc.receiveitem = rc.receiveitem
       AND ri.saleid = S.SALEID) AS LASTINSTALLMENT,
    (SELECT MAX(sa.installment)
     FROM XININSURE.SALEACTION sa
     WHERE sa.actionid = ssa.actionid
       AND sa.actionstatus = 'W'
       AND sa.saleid = S.SALEID
       AND sa.sequence = (SELECT MAX(sa2.sequence)
                          FROM XININSURE.SALEACTION sa2
                          WHERE sa2.actionid = ssa.actionid
                            AND sa2.actionstatus = 'W'
                            AND sa2.saleid = S.SALEID)) AS FOLLOWINSTALLMENT,
    S.PAYMENTMODE ||':'|| M.PAYMENTMODENAME AS PAYMENTMODE,
    (SELECT MIN(SP.INSTALLMENT)
     FROM XININSURE.SALEPAYMENT SP
     WHERE SP.SALEID = S.SALEID
       AND SP.BALANCE > 0) AS งวดที่กำหนดชำระ,
    (SELECT SP.BALANCE
     FROM XININSURE.SALEPAYMENT SP
     WHERE SP.SALEID = S.SALEID
       AND SP.INSTALLMENT = (SELECT MIN(SP2.INSTALLMENT)
                             FROM XININSURE.SALEPAYMENT SP2
                             WHERE SP2.SALEID = S.SALEID
                               AND SP2.BALANCE > 0)) AS ค้างชำระในงวด,
    NVL((SELECT s.BALANCE
         FROM XININSURE.SALEPAYMENT s
         WHERE s.saleid = ssa.saleid
           AND s.balance > 0
           AND s.installment = (SELECT sa.installment
                                FROM XININSURE.SALEACTION sa
                                WHERE sa.saleid = ssa.saleid
                                  AND sa.actionid = 44
                                  AND sa.actionstatus = 'W'
                                  AND sa.sequence = (SELECT MAX(sa2.sequence)
                                                     FROM XININSURE.SALEACTION sa2
                                                     WHERE sa2.saleid = ssa.saleid
                                                       AND sa2.actionid = 44
                                                       AND sa2.actionstatus = 'W'))), 0) AS BALANCE,
    S.PAIDAMOUNT AS รับชำระ,
    PT.PRODUCTGROUP,
    PT.PRODUCTTYPE
FROM
    XININSURE.SALE S,
    XININSURE.STAFF F,
    XININSURE.DEPARTMENT D,
    XININSURE.PRODUCT P,
    XININSURE.PRODUCTTYPE PT,
    XININSURE.PAYMENTMODE M,
    XININSURE.SUPPLIER SU,
    XININSURE.ROUTE R,
    XININSURE.BRANCH B,
    (SELECT
        SA.SALEID,
        SA.INSTALLMENT,
        A.ACTIONID,
        A.ACTIONCODE,
        SA.ACTIONREMARK,
        SA.REQUESTREMARK,
        SA.ACTIONSTATUS,
        SA.RESULTID,
        SA.DUEDATE,
        SA.SEQUENCE
     FROM XININSURE.SALEACTION SA,
          XININSURE.ACTION A
     WHERE 1=1
     AND SA.ACTIONID IN(3760, 3761, 2261, 3740, 3741, 3742, 3743,
	                    5933, 11293, 7533, 9133, 9174, 11574, 11575, 11576, 
	                    11577, 11553, 11554, 11555, 15014)
       AND SA.ACTIONID = A.ACTIONID
       AND SA.ACTIONSTATUS IN ('R', 'W', 'Y')
       AND SA.DUEDATE = TO_DATE('30/07/2024', 'DD/MM/YYYY')
       -- หรือใช้เงื่อนไขวันที่อื่นตามต้องการ
       -- AND SA.DUEDATE = TRUNC(SYSDATE) 
       -- AND SA.DUEDATE >= TO_DATE('25/07/2024', 'DD/MM/YYYY')
       -- AND SA.DUEDATE <= TO_DATE('30/07/2024', 'DD/MM/YYYY')
    ) SSA
WHERE
    S.SALEID = SSA.SALEID
    AND S.STAFFID = F.STAFFID
    AND S.STAFFDEPARTMENTID = D.DEPARTMENTID
    AND S.ROUTEID = R.ROUTEID
    AND S.PRODUCTID = P.PRODUCTID
    AND P.SUPPLIERID = SU.SUPPLIERID
    AND S.PRODUCTID = M.PRODUCTID
    AND S.PAYMENTMODE = M.PAYMENTMODE
    AND P.PRODUCTTYPE = PT.PRODUCTTYPE
--    AND B.BRANCHID = R.BRANCHID
    -- ใช้เงื่อนไข SUPPLIERCODE เหมือนกับ Query ที่สอง
--    AND SU.SUPPLIERCODE IN ('KWIL','AIA','SSL','BKI','BLA','VY','DHP','TVI','MTI','MTL','MLI','ALIFE','FWD','KTAL','ACE','SELIC','PLA','TSLI', 'ESY')
    AND S.SALESTATUS = 'C'
    AND S.PLATEID IS NULL
    AND S.CANCELDATE IS NULL
    AND S.CANCELEFFECTIVEDATE IS NULL
ORDER BY S.SALEID DESC