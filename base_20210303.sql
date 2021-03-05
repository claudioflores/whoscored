
# Selects lineups used in every game

SET @sql = NULL;
SET @team_id = 19584;

SELECT GROUP_CONCAT(DISTINCT CONCAT('SUM(IF(Position=''',Position,''',1,0)) ',Position)) 
INTO @sql
FROM whoscored.game_stats 
WHERE Team_ID = @team_id 
AND N BETWEEN 1 AND 11;

SET @sql = CONCAT('CREATE TEMPORARY TABLE whoscored.lineup 
				   SELECT ', @sql,' 
                   FROM whoscored.game_stats 
                   WHERE Team_ID = ',@team_id,' 
					AND N BETWEEN 1 AND 11 
				   GROUP BY GameID');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


# Groups most used lineups

#DROP TABLE whoscored.lineup_grouped;
SET @sql =  (SELECT GROUP_CONCAT(DISTINCT CONCAT(Position)) 
			FROM whoscored.game_stats 
			WHERE Team_ID = @team_id 
			AND N BETWEEN 1 AND 11);   
            
SET @sql = CONCAT('CREATE TABLE whoscored.lineup_grouped
				   SELECT ',@sql,',count(*) q, ROW_NUMBER() OVER (ORDER BY count(*) desc) q_order FROM whoscored.lineup GROUP BY ',@sql);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

# Pivot table of most common lineup

DROP TABLE whoscored.main_lineup;
CREATE TEMPORARY TABLE whoscored.main_lineup (Position varchar(5), Q int);

SELECT GROUP_CONCAT('(''',Position,''',(SELECT ',Position,' FROM whoscored.lineup_grouped WHERE q_order = 1))')
INTO @sql
FROM  whoscored.pos_players
WHERE pos_order = 1;

SET @sql = CONCAT('INSERT INTO whoscored.main_lineup VALUES ',@sql,';');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


# Count of positions played by players

#DROP TABLE whoscored.pos_players;
CREATE TEMPORARY TABLE whoscored.pos_players
SELECT Player_ID, 
	Position, 
    COUNT(*) Q, 
    ROW_NUMBER()  OVER (PARTITION BY Position ORDER BY COUNT(*) desc) pos_order,
    ROW_NUMBER() OVER (PARTITION BY Player_ID ORDER BY COUNT(*) desc) ply_order
FROM whoscored.game_stats
WHERE Team_ID = @team_id
	AND N between 1 and 11
GROUP BY Player_ID, Position;


# Stats of all players

#drop TABLE whoscored.player_summary;
CREATE TABLE whoscored.player_summary (Player_ID int, Position varchar(5), Games int, 
	Shots_Total float, ShotOnTarget float, KeyPassTotal float, PassSuccessInMatch float, DuelAerialWon float, Touches float, rating float, TackleWonTotal float,	InterceptionAll float, ClearanceTotal float, ShotBlocked float, 
    FoulCommitted float, OffsideGiven float, TurnOver float, Dispossessed float, TotalPasses float,	PassCrossTotal float, PassCrossAccurate float, PassLongBallTotal float, PassLongBallAccurate float, PassThroughBallTotal float, 
    PassThroughBallAccurate float,
	Percentile_Shots_Total float, Percentile_ShotOnTarget float, Percentile_KeyPassTotal float, Percentile_PassSuccessInMatch float, Percentile_DuelAerialWon float, Percentile_Touches float, Percentile_rating float, 
    Percentile_TackleWonTotal float, Percentile_InterceptionAll float, Percentile_ClearanceTotal float, Percentile_ShotBlocked float, Percentile_FoulCommitted float, Percentile_OffsideGiven float, Percentile_TurnOver float, 
    Percentile_Dispossessed float, Percentile_TotalPasses float, Percentile_PassCrossTotal float, Percentile_PassCrossAccurate float, Percentile_PassLongBallTotal float, Percentile_PassLongBallAccurate float, 
    Percentile_PassThroughBallTotal float, Percentile_PassThroughBallAccurate float);

#TRUNCATE TABLE whoscored.player_summary;
INSERT INTO whoscored.player_summary 
	(Player_ID, Position, Games, Shots_Total, Percentile_Shots_Total, ShotOnTarget, Percentile_ShotOnTarget, KeyPassTotal, Percentile_KeyPassTotal, PassSuccessInMatch, Percentile_PassSuccessInMatch, 
    DuelAerialWon, Percentile_DuelAerialWon, Touches, Percentile_Touches, rating, Percentile_rating, TackleWonTotal, Percentile_TackleWonTotal, InterceptionAll, Percentile_InterceptionAll, 
    ClearanceTotal, Percentile_ClearanceTotal, ShotBlocked, Percentile_ShotBlocked, FoulCommitted, Percentile_FoulCommitted, OffsideGiven, Percentile_OffsideGiven, TurnOver, Percentile_TurnOver, 
    Dispossessed, Percentile_Dispossessed, TotalPasses, Percentile_TotalPasses, PassCrossTotal, Percentile_PassCrossTotal, PassCrossAccurate, Percentile_PassCrossAccurate, PassLongBallTotal, Percentile_PassLongBallTotal, 
    PassLongBallAccurate, Percentile_PassLongBallAccurate, PassThroughBallTotal, Percentile_PassThroughBallTotal, PassThroughBallAccurate, Percentile_PassThroughBallAccurate)
SELECT Player_ID, Position,	COUNT(*) Games,	
	AVG(ShotsTotal) ShotsTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(ShotsTotal) ASC) Percentile_Shots_Total,
    AVG(ShotOnTarget) ShotOnTarget, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(ShotOnTarget) ASC) Percentile_ShotOnTarget,
    AVG(KeyPassTotal) KeyPassTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(KeyPassTotal) ASC) Percentile_KeyPassTotal,
    round(SUM(TotalPasses*PassSuccessInMatch/100),0)/sum(TotalPasses) PassSuccessInMatch, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY round(SUM(TotalPasses*PassSuccessInMatch/100),0)/sum(TotalPasses) ASC) Percentile_PassSuccessInMatch,
    AVG(DuelAerialWon) DuelAerialWon, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(DuelAerialWon) ASC) Percentile_DuelAerialWon,
    AVG(Touches) Touches, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(Touches) ASC) Percentile_Touches,
    AVG(rating) rating, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(rating) ASC) Percentile_rating,
    AVG(TackleWonTotal) TackleWonTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(TackleWonTotal) ASC) Percentile_TackleWonTotal,
    AVG(InterceptionAll) InterceptionAll, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(InterceptionAll) ASC) Percentile_InterceptionAll,
    AVG(ClearanceTotal) ClearanceTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(ClearanceTotal) ASC) Percentile_ClearanceTotal,
    AVG(ShotBlocked) ShotBlocked, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(ShotBlocked) ASC) Percentile_ShotBlocked,
    AVG(FoulCommitted) FoulCommitted, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(FoulCommitted) ASC) Percentile_FoulCommitted,
    AVG(OffsideGiven) OffsideGiven, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(OffsideGiven) ASC) Percentile_OffsideGiven,
    AVG(TurnOver) TurnOver, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(TurnOver) ASC) Percentile_TurnOver,
    AVG(Dispossessed) Dispossessed, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(Dispossessed) ASC) Percentile_Dispossessed,
    AVG(TotalPasses) TotalPasses, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(TotalPasses) ASC) Percentile_TotalPasses,
    AVG(PassCrossTotal) PassCrossTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassCrossTotal) ASC) Percentile_PassCrossTotal,
    AVG(PassCrossAccurate) PassCrossAccurate, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassCrossAccurate) ASC) Percentile_PassCrossAccurate,
    AVG(PassLongBallTotal) PassLongBallTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassLongBallTotal) ASC) Percentile_PassLongBallTotal,
    AVG(PassLongBallAccurate) PassLongBallAccurate, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassLongBallAccurate) ASC) Percentile_PassLongBallAccurate,
    AVG(PassThroughBallTotal) PassThroughBallTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassThroughBallTotal) ASC) Percentile_PassThroughBallTotal,
    AVG(PassThroughBallAccurate) PassThroughBallAccurate, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassThroughBallAccurate) ASC) Percentile_PassThroughBallAccurate
FROM whoscored.game_stats
WHERE N BETWEEN 1 AND 11
	AND Position <> ''
GROUP BY Player_ID, Position;

    
# Selection of most common players used in main lineup

CREATE TEMPORARY TABLE whoscored.main_lineup_stats
SELECT t1.Position, t2.Player_ID, t3.Games, 
	t3.Shots_Total, t3.ShotOnTarget, t3.KeyPassTotal, t3.PassSuccessInMatch, t3.DuelAerialWon, t3.Touches, t3.rating, t3.TackleWonTotal, t3.InterceptionAll, t3.ClearanceTotal, t3.ShotBlocked, t3.FoulCommitted, t3.OffsideGiven, 
    t3.TurnOver, t3.Dispossessed, t3.TotalPasses, t3.PassCrossTotal, t3.PassCrossAccurate, t3.PassLongBallTotal, t3.PassLongBallAccurate, t3.PassThroughBallTotal, t3.PassThroughBallAccurate,
    t3.Percentile_Shots_Total, t3.Percentile_ShotOnTarget, t3.Percentile_KeyPassTotal, t3.Percentile_PassSuccessInMatch, t3.Percentile_DuelAerialWon, t3.Percentile_Touches, t3.Percentile_rating, t3.Percentile_TackleWonTotal, 
    t3.Percentile_InterceptionAll, t3.Percentile_ClearanceTotal, t3.Percentile_ShotBlocked, t3.Percentile_FoulCommitted, t3.Percentile_OffsideGiven, t3.Percentile_TurnOver, t3.Percentile_Dispossessed, t3.Percentile_TotalPasses, 
    t3.Percentile_PassCrossTotal, t3.Percentile_PassCrossAccurate, t3.Percentile_PassLongBallTotal, t3.Percentile_PassLongBallAccurate, t3.Percentile_PassThroughBallTotal, t3.Percentile_PassThroughBallAccurate
FROM whoscored.main_lineup t1
JOIN whoscored.pos_players t2
	on t1.Position = t2.Position
join whoscored.player_summary t3
	on t2.Player_ID = t3.Player_ID
    and t2.Position = t3.Position
WHERE t1.Q > 0
	and t2.pos_order <= t1.Q
ORDER BY t1.Position;

SELECT * FROM whoscored.main_lineup_stats;

DROP TABLE whoscored.kpis_position;
CREATE TABLE whoscored.kpis_position (Position varchar(5), N_KPI varchar(6), KPI varchar(50));
INSERT INTO whoscored.kpis_position VALUES ('GK','KPI_1','Percentile_PassSuccessInMatch'),('GK','KPI_2','Percentile_DuelAerialWon'),('GK','KPI_3','Percentile_ClearanceTotal'),('GK','KPI_4','Percentile_PassLongBAllAccurate'),('GK','KPI_5','Percentile_TotalPasses'),('GK','KPI_6','Percentile_Touches');
INSERT INTO whoscored.kpis_position VALUES ('DC','KPI_1','Percentile_PassSuccessInMatch'),('DC','KPI_2','Percentile_DuelAerialWon'),('DC','KPI_3','Percentile_ClearanceTotal'),('DC','KPI_4','Percentile_PassLongBAllAccurate'),('DC','KPI_5','Percentile_TotalPasses'),('DC','KPI_6','Percentile_Touches');
INSERT INTO whoscored.kpis_position VALUES ('DL','KPI_1','Percentile_PassSuccessInMatch'),('DL','KPI_2','Percentile_DuelAerialWon'),('DL','KPI_3','Percentile_ClearanceTotal'),('DL','KPI_4','Percentile_PassLongBAllAccurate'),('DL','KPI_5','Percentile_TotalPasses'),('DL','KPI_6','Percentile_Touches');
INSERT INTO whoscored.kpis_position VALUES ('DR','KPI_1','Percentile_PassSuccessInMatch'),('DR','KPI_2','Percentile_DuelAerialWon'),('DR','KPI_3','Percentile_ClearanceTotal'),('DR','KPI_4','Percentile_PassLongBAllAccurate'),('DR','KPI_5','Percentile_TotalPasses'),('DR','KPI_6','Percentile_Touches');
INSERT INTO whoscored.kpis_position VALUES ('DMC','KPI_1','Percentile_PassSuccessInMatch'),('DMC','KPI_2','Percentile_DuelAerialWon'),('DMC','KPI_3','Percentile_ClearanceTotal'),('DMC','KPI_4','Percentile_PassLongBAllAccurate'),('DMC','KPI_5','Percentile_TotalPasses'),('DMC','KPI_6','Percentile_Touches');
INSERT INTO whoscored.kpis_position VALUES ('AMC','KPI_1','Percentile_PassSuccessInMatch'),('AMC','KPI_2','Percentile_DuelAerialWon'),('AMC','KPI_3','Percentile_ClearanceTotal'),('AMC','KPI_4','Percentile_PassLongBAllAccurate'),('AMC','KPI_5','Percentile_TotalPasses'),('AMC','KPI_6','Percentile_Touches');
INSERT INTO whoscored.kpis_position VALUES ('AML','KPI_1','Percentile_PassSuccessInMatch'),('AML','KPI_2','Percentile_DuelAerialWon'),('AML','KPI_3','Percentile_ClearanceTotal'),('AML','KPI_4','Percentile_PassLongBAllAccurate'),('AML','KPI_5','Percentile_TotalPasses'),('AML','KPI_6','Percentile_Touches');
INSERT INTO whoscored.kpis_position VALUES ('AMR','KPI_1','Percentile_PassSuccessInMatch'),('AMR','KPI_2','Percentile_DuelAerialWon'),('AMR','KPI_3','Percentile_ClearanceTotal'),('AMR','KPI_4','Percentile_PassLongBAllAccurate'),('AMR','KPI_5','Percentile_TotalPasses'),('AMR','KPI_6','Percentile_Touches');
INSERT INTO whoscored.kpis_position VALUES ('FW','KPI_1','Percentile_PassSuccessInMatch'),('FW','KPI_2','Percentile_DuelAerialWon'),('FW','KPI_3','Percentile_ClearanceTotal'),('FW','KPI_4','Percentile_PassLongBAllAccurate'),('FW','KPI_5','Percentile_TotalPasses'),('FW','KPI_6','Percentile_Touches');


DROP TABLE whoscored.kpis_position_main6;
CREATE TABLE whoscored.kpis_position_main6 (Player_ID int, N int, Position varchar(5), Games int, KPI_1 float, KPI_2 float, KPI_3 float, KPI_4 float, KPI_5 float, KPI_6 float);
TRUNCATE TABLE whoscored.kpis_position_main6;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'GK';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 1, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','GK',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'DC';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 2, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','DC',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'DL';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 3, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','DL',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'DR';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 4, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','DR',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'DMC';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 5, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','DMC',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'AMC';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 6, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','AMC',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'AML';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 7, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','AML',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'AMR';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 8, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','AMR',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'FW';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 9, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','FW',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

DROP TABLE whoscored.player_team;
CREATE TABLE whoscored.player_team (Player_ID int, Player_Name varchar(50), Team varchar(50), Date_Order int)
SELECT t2.Player_ID, t2.Player_Name, t2.Team, ROW_NUMBER() OVER (PARTITION BY t2.Player_ID ORDER BY t1.Date desc) Date_Order
from whoscored.games t1
join whoscored.game_stats  t2
	on t1.GameID = t2.GameID;

DELETE FROM  whoscored.player_team WHERE Date_Order > 1;

DROP TABLE whoscored.player_replacements;
CREATE TABLE whoscored.player_replacements
SELECT t2.N, t2.Position, t2.Player_ID, t4.Player_Name, t4.Team, t2.Games, t2.KPI_1, t2.KPI_2, t2.KPI_3, t2.KPI_4, t2.KPI_5, t2.KPI_6, 
	(t2.KPI_1 + t2.KPI_2 + t2.KPI_3 + t2.KPI_4 + t2.KPI_5 + t2.KPI_6)/6 AVG_KPI, 
	t3.Player_ID Player_ID_rep, t5.Player_Name Player_Name_Rep, t5.Team Team_Rep, t3.Games Games_rep, t3.KPI_1 KPI_1_rep, t3.KPI_2 KPI_2_rep, t3.KPI_3 KPI_3_rep, t3.KPI_4 KPI_4_rep, t3.KPI_5 KPI_5_rep, t3.KPI_6 KPI_6_rep,
    (t3.KPI_1 + t3.KPI_2 + t3.KPI_3 + t3.KPI_4 + t3.KPI_5 + t3.KPI_6)/6 AVG_KPI_rep,
    (POWER(CASE WHEN t3.KPI_1<t2.KPI_1 THEN ABS(t3.KPI_1-t2.KPI_1) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_2<t2.KPI_2 THEN ABS(t3.KPI_2-t2.KPI_2) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_3<t2.KPI_3 THEN ABS(t3.KPI_3-t2.KPI_3) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_4<t2.KPI_4 THEN ABS(t3.KPI_4-t2.KPI_4) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_5<t2.KPI_5 THEN ABS(t3.KPI_5-t2.KPI_5) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_6<t2.KPI_6 THEN ABS(t3.KPI_6-t2.KPI_6) ELSE 0 END,2))/6 Difference,
    ROW_NUMBER() OVER (PARTITION BY t2.Position, t2.Player_ID ORDER BY (t3.KPI_1 + t3.KPI_2 + t3.KPI_3 + t3.KPI_4 + t3.KPI_5 + t3.KPI_6)/6 DESC, (POWER(CASE WHEN t3.KPI_1<t2.KPI_1 THEN ABS(t3.KPI_1-t2.KPI_1) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_2<t2.KPI_2 THEN ABS(t3.KPI_2-t2.KPI_2) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_3<t2.KPI_3 THEN ABS(t3.KPI_3-t2.KPI_3) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_4<t2.KPI_4 THEN ABS(t3.KPI_4-t2.KPI_4) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_5<t2.KPI_5 THEN ABS(t3.KPI_5-t2.KPI_5) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_6<t2.KPI_6 THEN ABS(t3.KPI_6-t2.KPI_6) ELSE 0 END,2))/6 ASC) Difference_Order
FROM whoscored.main_lineup_stats t1
JOIN whoscored.kpis_position_main6 t2 
	ON t1.Position = t2.Position
    AND t1.Player_ID = t2.Player_ID
LEFT JOIN whoscored.kpis_position_main6 t3
	ON t2.Position = t3.Position
    AND t2.Player_ID <> t3.Player_ID
LEFT JOIN whoscored.player_team t4
	ON t1.Player_ID = t4.Player_ID
LEFT JOIN whoscored.player_team t5
	ON t3.Player_ID = t5.Player_ID
where t4.Date_Order = 1
	and t5.Date_Order = 1;


SELECT *
FROM whoscored.kpis_position;

SELECT * 
FROM whoscored.player_replacements
WHERE Difference_Order <= 5
ORDER BY N, Player_ID, Difference_Order;
