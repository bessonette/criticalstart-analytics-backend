<?php

$host = 'www.vomo.test';
$db   = 'cs_salesforce';
$user = 'homestead';
$pass = 'secret';
$charset = 'utf8mb4';

$dsn = "mysql:host=$host;dbname=$db;charset=$charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
];
try {
     $pdo = new PDO($dsn, $user, $pass, $options);
} catch (\PDOException $e) {
     throw new \PDOException($e->getMessage(), (int)$e->getCode());
}

// $stmt = $pdo->query("SELECT `Lead Source`, `Referrer Code`, New_SF_Program, New_SF_Traffic_Source, New_SF_Lead_Source, New_SF_Referrer_Code, count(*) as leads
// FROM cs_salesforce.lead
// WHERE Converted = 0
// AND `Created Date` >= '2020-02-01'
// AND `Record Type ID` = '0123j000000X9c3AAC'
// GROUP BY `Lead Source`, `Referrer Code`, New_SF_Program, New_SF_Traffic_Source, New_SF_Lead_Source, New_SF_Referrer_Code
// ORDER BY `Lead Source`, `Referrer Code`, New_SF_Program, New_SF_Traffic_Source, New_SF_Lead_Source, New_SF_Referrer_Code");

// $output = array();
// while ($row = $stmt->fetch()) {
// 	$output[] = $row;
// }

if (!isset($_GET['debug'])) {
	header('Access-control-allow-origin: *');
	header('Content-type: text/json');
}

$metric = $_GET['metric'];
$start = $_GET['start'];
$end = $_GET['end'];

$events = false;
if (isset($_GET['events'])) {
	$events = $_GET['events'];
}

$datediff = (strtotime($end) - strtotime($start)) / (60*60*24);

if ($datediff < 100) {
	$period = '3 month';
} else {
	$period = '1 year';
}

// First day of the last month.
$priorStart = date('Y-m-01', strtotime('-' . $period, strtotime($start)));
//echo $priorStart;

// Last day of the last month.
$priorEnd = date('Y-m-t', strtotime('-' . $period, strtotime($end)));
//echo $priorEnd;

$eventsSQL = '';
$eventsSQLLeadTable = '';

if ($events == 'true') {
	$eventsSQL =" AND `Lead Source` <> 'Event' ";
	$eventsSQLLeadTable =" AND lead.`Lead Source` <> 'Event' ";
}

switch ($metric) {
	case 'itppageviews':
		$resultsRollup = array();
		$stmt = $pdo->prepare("SELECT SUM(ga_sessions) as sessions FROM cs_salesforce.ga_sessions WHERE ga_date BETWEEN :start AND :end");
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['current'] = $results['sessions'];

		$stmt = $pdo->prepare("SELECT SUM(ga_sessions) as sessions FROM cs_salesforce.ga_sessions WHERE ga_date BETWEEN :start AND :end");
		$stmt->execute(array(':start' => $priorStart . ' 00:00:00', ':end' => $priorEnd . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['prior'] = $results['sessions'];

		echo json_encode($resultsRollup);
		//print_r($results);
		//echo json_encode($output);

		break;

	case 'itpinquiries':
		$resultsRollup = array();
		$stmt = $pdo->prepare("SELECT count(*) as metric FROM cs_salesforce.lead WHERE `Created Date` BETWEEN :start AND :end AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL) " . $eventsSQL);
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['current'] = $results['metric'];

		$stmt = $pdo->prepare("SELECT count(*) as metric, `Lead Source` as lead_sources FROM cs_salesforce.lead WHERE `Created Date` BETWEEN :start AND :end AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)" . $eventsSQL . ' GROUP BY `Lead Source`');
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetchAll();

		$resultsRollup['lead_sources'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as metric FROM cs_salesforce.lead WHERE `Created Date` BETWEEN :start AND :end AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)" . $eventsSQL);
		$stmt->execute(array(':start' => $priorStart . ' 00:00:00', ':end' => $priorEnd . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['prior'] = $results['metric'];

		echo json_encode($resultsRollup);

		break;

	case 'itpmqls':
		$resultsRollup = array();
		$stmt = $pdo->prepare("SELECT count(*) as metric FROM cs_salesforce.lead WHERE  `Lead Source` NOT IN ('Event')AND `MQL Date` >= :start AND `MQL Date` <= :end" . $eventsSQL);
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['current'] = $results['metric'];

		$stmt = $pdo->prepare("SELECT count(*) as metric, `Lead Source` as lead_sources FROM cs_salesforce.lead WHERE `Referrer Code` NOT LIKE 'Event%' AND `MQL Date` >= :start AND `MQL Date` <= :end" . $eventsSQL . ' GROUP BY `Lead Source` ORDER BY metric DESC');;
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetchAll();

		$resultsRollup['lead_sources'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as metric FROM cs_salesforce.lead WHERE `Referrer Code` NOT LIKE 'Event%' AND `MQL Date` >= :start AND `MQL Date` <= :end" . $eventsSQL);
		$stmt->execute(array(':start' => $priorStart . ' 00:00:00', ':end' => $priorEnd . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['prior'] = $results['metric'];

		echo json_encode($resultsRollup);

		break;

	case 'itpsals':
		$resultsRollup = array();
		$stmt = $pdo->prepare("SELECT count(*) as metric FROM cs_salesforce.lead WHERE (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL) AND `SAL Date` >= :start AND `SAL Date` <= :end" . $eventsSQL);
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['current'] = $results['metric'];

		$stmt = $pdo->prepare("SELECT count(*) as metric, `Lead Source` as lead_sources FROM cs_salesforce.lead WHERE (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL) AND `SAL Date` >= :start AND `SAL Date` <= :end" . $eventsSQL . ' GROUP BY `Lead Source` ORDER BY metric DESC');;
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetchAll();

		$resultsRollup['lead_sources'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as metric FROM cs_salesforce.lead WHERE (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL) AND `SAL Date` >= :start AND `SAL Date` <= :end" . $eventsSQL);
		$stmt->execute(array(':start' => $priorStart . ' 00:00:00', ':end' => $priorEnd . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['prior'] = $results['metric'];

		echo json_encode($resultsRollup);

		break;

	case 'itpsqls':
		$resultsRollup = array();
		$stmt = $pdo->prepare("SELECT count(*) as metric FROM cs_salesforce.lead WHERE (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL) AND `SQL Date` >= :start AND `SQL Date` <= :end" . $eventsSQL);
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['current'] = $results['metric'];

		$stmt = $pdo->prepare("SELECT count(*) as metric, `Lead Source` as lead_sources FROM cs_salesforce.lead WHERE (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL) AND `SQL Date`  >= :start AND `SQL Date` <= :end" . $eventsSQL . ' GROUP BY `Lead Source` ORDER BY metric DESC');;
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetchAll();

		$resultsRollup['lead_sources'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as metric FROM cs_salesforce.lead WHERE (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL) AND `SQL Date` >= :start AND `SQL Date` <= :end" . $eventsSQL);
		$stmt->execute(array(':start' => $priorStart . ' 00:00:00', ':end' => $priorEnd . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['prior'] = $results['metric'];

		echo json_encode($resultsRollup);

		break;

	case 'itpCSGenPipeline':
		$resultsRollup = array();
		$stmt = $pdo->prepare("SELECT count(*) as deals, sum(amount) as total FROM opportunity
			INNER JOIN opportunity_contact_role opp_role ON opportunity.`Opportunity ID` = opp_role.`Opportunity ID`
			WHERE opportunity.`Created Date` >= :start AND opportunity.`Created Date` <= :end
			AND MQL = 1
			AND `Opportunity Type` = 'New Business'
			AND `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit')
			AND `From Internal Account` = 0
			AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)
			AND (`Lead Source` NOT LIKE '%Channel%' OR `Lead Source` IS NULL)" . $eventsSQL);
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['current'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as metric, `Lead Source` as lead_sources FROM opportunity
			INNER JOIN opportunity_contact_role opp_role ON opportunity.`Opportunity ID` = opp_role.`Opportunity ID`
			WHERE opportunity.`Created Date` >= :start AND opportunity.`Created Date` <= :end
			AND MQL = 1
			AND `Opportunity Type` = 'New Business'
			AND `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit')
			AND `From Internal Account` = 0
			AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)
			AND (`Lead Source` NOT LIKE '%Channel%' OR `Lead Source` IS NULL)" . $eventsSQL . ' GROUP BY `Lead Source` ORDER BY metric DESC');

		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetchAll();

		$resultsRollup['lead_sources'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as deals, sum(amount) as total FROM opportunity
			INNER JOIN opportunity_contact_role opp_role ON opportunity.`Opportunity ID` = opp_role.`Opportunity ID`
			WHERE opportunity.`Created Date` >= :start AND opportunity.`Created Date` <= :end
			AND MQL = 1
			AND `Opportunity Type` = 'New Business'
			AND `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit')
			AND `From Internal Account` = 0
			AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)
			AND (`Lead Source` NOT LIKE '%Channel%' OR `Lead Source` IS NULL)" . $eventsSQL);
		$stmt->execute(array(':start' => $priorStart . ' 00:00:00', ':end' => $priorEnd . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['prior'] = $results;

		echo json_encode($resultsRollup);

		break;

	case 'itpChannelGenPipeline':
		$resultsRollup = array();
		$stmt = $pdo->prepare("SELECT count(*) as deals, sum(amount) as total FROM opportunity
			INNER JOIN opportunity_contact_role opp_role ON opportunity.`Opportunity ID` = opp_role.`Opportunity ID`
			WHERE opportunity.`Created Date` >= :start AND opportunity.`Created Date` <= :end
			AND MQL = 1
			AND `Opportunity Type` = 'New Business'
			AND `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit')
			AND `From Internal Account` = 0
			AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)
			AND `Lead Source` LIKE '%Channel%'" . $eventsSQL);
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['current'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as metric, `Lead Source` as lead_sources FROM opportunity
			INNER JOIN opportunity_contact_role opp_role ON opportunity.`Opportunity ID` = opp_role.`Opportunity ID`
			WHERE opportunity.`Created Date` >= :start AND opportunity.`Created Date` <= :end
			AND MQL = 1
			AND `Opportunity Type` = 'New Business'
			AND `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit')
			AND `From Internal Account` = 0
			AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)
			AND `Lead Source` LIKE '%Channel%'" . $eventsSQL . ' GROUP BY `Lead Source` ORDER BY metric DESC');

		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetchAll();

		$resultsRollup['lead_sources'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as deals, sum(amount) as total FROM opportunity
			INNER JOIN opportunity_contact_role opp_role ON opportunity.`Opportunity ID` = opp_role.`Opportunity ID`
			WHERE opportunity.`Created Date` >= :start AND opportunity.`Created Date` <= :end
			AND MQL = 1
			AND `Opportunity Type` = 'New Business'
			AND `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit')
			AND `From Internal Account` = 0
			AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)
			AND `Lead Source` LIKE '%Channel%'" . $eventsSQL);
		$stmt->execute(array(':start' => $priorStart . ' 00:00:00', ':end' => $priorEnd . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['prior'] = $results;

		echo json_encode($resultsRollup);

		break;

	case 'itpClosedWon':
		$resultsRollup = array();
		$stmt = $pdo->prepare("SELECT count(*) as deals, sum(amount) as total FROM opportunity
			INNER JOIN opportunity_contact_role opp_role ON opportunity.`Opportunity ID` = opp_role.`Opportunity ID`
			WHERE opportunity.`Created Date` >= :start AND opportunity.`Created Date` <= :end
			AND MQL = 1
			AND `Opportunity Type` = 'New Business'
			AND `Stage` = 'Closed Won'
			AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)
			AND `From Internal Account` = 0" . $eventsSQL);
		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['current'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as metric, `Lead Source` as lead_sources FROM opportunity
			INNER JOIN opportunity_contact_role opp_role ON opportunity.`Opportunity ID` = opp_role.`Opportunity ID`
			WHERE opportunity.`Created Date` >= :start AND opportunity.`Created Date` <= :end
			AND MQL = 1
			AND `Opportunity Type` = 'New Business'
			AND `Stage` = 'Closed Won'
			AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)
			AND `From Internal Account` = 0" . $eventsSQL . ' GROUP BY `Lead Source` ORDER BY metric DESC');

		$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
		$results = $stmt->fetchAll();

		$resultsRollup['lead_sources'] = $results;

		$stmt = $pdo->prepare("SELECT count(*) as deals, sum(amount) as total FROM opportunity
			INNER JOIN opportunity_contact_role opp_role ON opportunity.`Opportunity ID` = opp_role.`Opportunity ID`
			WHERE opportunity.`Created Date` >= :start AND opportunity.`Created Date` <= :end
			AND MQL = 1
			AND `Opportunity Type` = 'New Business'
			AND `Stage` = 'Closed Won'
			AND (`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR `Lead Source` IS NULL)
			AND `From Internal Account` = 0" . $eventsSQL);
		$stmt->execute(array(':start' => $priorStart . ' 00:00:00', ':end' => $priorEnd . ' 23:59:59'));
		$results = $stmt->fetch();

		$resultsRollup['prior'] = $results;

		echo json_encode($resultsRollup);

		break;


	case 'getRowDetails':
		$details = array();

		$dynamicRepQuery = '';
		$dynamicMetricWhere = '';
		switch ($_GET['detail_metric']) {
			// case 'mql':
			// 	$dynamicMetricWhere = '';
			// 	break;

			case 'mql':
				$dynamicMetricWhere = "AND mql_date IS NOT NULL AND lead_source != 'Event'";
				break;

			case 'sal':
				$dynamicMetricWhere = 'AND sal_date IS NOT NULL';
				break;

			case 'sql':
				$dynamicMetricWhere = 'AND sql_date IS NOT NULL';
				break;

			case 'pipeline':
				$dynamicMetricWhere = "AND `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit')";
				break;

			case 'closed-won':
				$dynamicMetricWhere = "AND `Stage` IN ('Closed Won')";
				break;

			case 'closed-lost':
				$dynamicMetricWhere = "AND `Stage` IN ('Closed Lost')";
				break;
		}

		$dynamicBreakdownWhere = '';
		if (isset($_GET['breakdown'])) {
			switch ($_GET['breakdown']) {
				case 'leadsource':
					if ($_GET['breakout'] == 'PENDING') {
						$dynamicBreakdownWhere = "AND lead_source IS NULL";
					} elseif ($_GET['breakout'] == 'rollup') {
						$dynamicBreakdownWhere = "";
					} else {
						$dynamicBreakdownWhere = "AND lead_source = '" . $_GET['breakout'] . "'";
					}

					break;

				case 'leadsource-w-referrer':
					$breakout = explode('||', $_GET['breakout']);

					$tempLeadSource = '';
					if ($breakout[0] == 'PENDING') {
						$tempLeadSource = "lead_source IS NULL";
					} else {
						$tempLeadSource = "lead_source = '" . $breakout[0] . "'";
					}

					if ($_GET['breakout'] != 'rollup') {
						$dynamicBreakdownWhere = "AND $tempLeadSource AND referrer_code = '" . $breakout[1] . "'";
					}

					break;

				case 'rep':
					if ($_GET['breakout'] == 'rollup') {
						$dynamicRepQuery = "";
					} else {
						$dynamicRepQuery = "AND owner_id = '" . $_GET['breakout'] . "'";
					}
					break;
			}
		}

		if ($dynamicBreakdownWhere == '') {
			$leadSourceWhere = '';

			$leadSourcesOrig = explode(',', $_GET['leadSources']);

			$leadSources = implode("', '", $leadSourcesOrig);

			$dynamicBreakdownWhere = "lead_source IN ('$leadSources')";

			if (stristr($_GET['leadSources'], 'Pending')) {
				$dynamicBreakdownWhere = '(' . $dynamicBreakdownWhere . ' OR lead_source IS NULL)';
			}

			$dynamicBreakdownWhere = 'AND ' . $dynamicBreakdownWhere;
		}

		//var_dump($dynamicBreakdownWhere); exit();

		switch ($_GET['detail_metric']) {
			case 'mql':
			case 'sal':
			case 'sql':

				/*
				$stmt = $pdo->prepare("SELECT
						lead.`First Name` as first_name,
						lead.`Last Name` as last_name,
						lead.`Lead ID` as lead_id,
						lead.Company as company,
						lead.Email as email,
						lead.`State/Province Code` as state,
						user.`First Name` as rep_first_name,
						user.`Last Name` as rep_last_name
					FROM lead
					INNER JOIN user ON lead.`Owner ID` = user.`User ID`
					WHERE
					lead.`Created Date` >= :start AND lead.`Created Date` <= :end
					$dynamicMetricWhere
					$dynamicBreakdownWhere
					$dynamicRepQuery
					ORDER BY lead.`Created Date` ASC");
				$stmt->execute(array(':start' => $_GET['start'], ':end' => $_GET['end']));
				$results = $stmt->fetchAll();
				*/


				$stmt = $pdo->prepare("
				SELECT
						type,
						first_name,
						last_name,
						lead_id,
						company,
						rollup.email,
						state,
						user.`First Name` as rep_first_name,
						user.`Last Name` as rep_last_name
				FROM (
					SELECT
						'Lead' as type,
						lead.`First Name` as first_name,
						lead.`Last Name` as last_name,
						lead.`Lead ID` as lead_id,
						lead.Company as company,
						lead.Email as email,
						lead.`State/Province Code` as state,
						lead.`Created Date` as created_date,
						`Lead Source` as lead_source,
						`Referrer Code` as referrer_code,
						`MQL Date` as mql_date,
						`SAL Date` as sal_date,
						`SQL Date` as sql_date,
						`Owner ID` as owner_id
					FROM lead
					WHERE
						`Converted` = 0
					UNION
					SELECT
						'Contact' as type,
						contact.`First Name` as first_name,
						contact.`Last Name` as last_name,
						contact.`Contact ID` as lead_id,
						contact.`Account Name` as company,
						contact.Email as email,
						contact.`Mailing State/Province Code` as state,
						contact.`Created Date` as created_date,
						`Lead Source` as lead_source,
						`Referrer Code` as referrer_code,
						`MQL Date` as mql_date,
						`SAL Date` as sal_date,
						`SQL Date` as sql_date,
						`Owner ID` as owner_id
					FROM contact
				) as rollup
				INNER JOIN user ON rollup.owner_id = user.`User ID`
				WHERE
					created_date >= :start AND created_date <= :end
					$dynamicMetricWhere
					$dynamicBreakdownWhere
					$dynamicRepQuery
				ORDER BY created_date ASC");
				$stmt->execute(array(':start' => $_GET['start'], ':end' => $_GET['end']));
				$results = $stmt->fetchAll();



				echo json_encode($results);

				break;

			case 'pipeline':
			case 'closed-won':
			case 'closed-lost':

				/*
				$stmt = $pdo->prepare("SELECT
						lead.`First Name` as first_name,
						lead.`Last Name` as last_name,
						lead.`Lead ID` as lead_id,
						lead.Company as company,
						lead.Email as email,
						lead.`State/Province Code` as state,
						user.`First Name` as rep_first_name,
						user.`Last Name` as rep_last_name,
						opp.amount as amount,
						opp.`Opportunity ID` as opp_id,
						opp.Department as opp_dept
					FROM lead
					LEFT JOIN opportunity_contact_role opp_role ON lead.`Converted Contact ID` = opp_role.`Contact ID`
					LEFT JOIN opportunity opp ON opp.`Opportunity ID` = opp_role.`Opportunity ID`
					LEFT JOIN user ON lead.`Owner ID` = user.`User ID`
					WHERE
					lead.`Created Date` >= :start AND lead.`Created Date` <= :end
					$dynamicMetricWhere
					$dynamicBreakdownWhere
					$dynamicRepQuery
					ORDER BY lead.`Created Date` ASC");
				$stmt->execute(array(':start' => $_GET['start'], ':end' => $_GET['end']));
				$results = $stmt->fetchAll();
				*/


				$stmt = $pdo->prepare("SELECT
						first_name,
						last_name,
						lead_id,
						company,
						rollup.Email as email,
						state,
						user.`First Name` as rep_first_name,
						user.`Last Name` as rep_last_name,
						opp.amount as amount,
						opp.`Opportunity ID` as opp_id,
						opp.Department as opp_dept
					FROM
						(
							SELECT
								'Lead' as type,
								lead.`First Name` as first_name,
								lead.`Last Name` as last_name,
								lead.`Lead ID` as lead_id,
								lead.Company as company,
								lead.Email as email,
								lead.`State/Province Code` as state,
								lead.`Converted Contact ID` as contact_id,
								lead.`Converted Account ID` as account_id,
								`Created Date` as created_date,
								`Lead Source` as lead_source,
								`Referrer Code` as referrer_code,
								`Owner ID` as owner_id
							FROM lead
							WHERE
								`Converted` = 0
							UNION
							SELECT
								'Contact' as type,
								contact.`First Name` as first_name,
								contact.`Last Name` as last_name,
								contact.`Contact ID` as lead_id,
								contact.`Account Name` as company,
								contact.Email as email,
								contact.`Mailing State/Province Code` as state,
								contact.`Contact ID` as contact_id,
								contact.`Account ID` as account_id,
								`Created Date` as created_date,
								`Lead Source` as lead_source,
								`Referrer Code` as referrer_code,
								`Owner ID` as owner_id
							FROM contact
						) as rollup
						LEFT JOIN opportunity_contact_role opp_role ON rollup.contact_id = opp_role.`Contact ID`
						LEFT JOIN opportunity opp ON opp.`Opportunity ID` = opp_role.`Opportunity ID` AND opp.`From Internal Account` = 0
							AND opp.`Opportunity Type` IN ('New Business', 'Opportunity')
						LEFT JOIN user ON rollup.owner_id = user.`User ID`
					WHERE
					rollup.created_date >= :start AND rollup.created_date <= :end
					$dynamicMetricWhere
					$dynamicBreakdownWhere
					$dynamicRepQuery
					ORDER BY rollup.created_date ASC");
				$stmt->execute(array(':start' => $_GET['start'], ':end' => $_GET['end']));
				$results = $stmt->fetchAll();

				if (isset($_GET['marketingInfluenced']) && $_GET['marketingInfluenced'] == 'true') {
					$stmt = $pdo->prepare("
								SELECT
									first_name,
									last_name,
									lead_id,
									company,
									rollup.Email as email,
									state,
									user.`First Name` as rep_first_name,
									user.`Last Name` as rep_last_name,
									opp.amount as amount,
									opp.`Opportunity ID` as opp_id,
									opp.Department as opp_dept
								FROM (
										SELECT
											'Lead' as type,
											lead.`First Name` as first_name,
											lead.`Last Name` as last_name,
											lead.`Lead ID` as lead_id,
											lead.Company as company,
											lead.Email as email,
											lead.`State/Province Code` as state,
											lead.`Converted Contact ID` as contact_id,
											lead.`Converted Account ID` as account_id,
											`Created Date` as created_date,
											`Lead Source` as lead_source,
											`Referrer Code` as referrer_code,
											`Owner ID` as owner_id
										FROM lead
										WHERE
											`Converted` = 0
										UNION
										SELECT
											'Contact' as type,
											contact.`First Name` as first_name,
											contact.`Last Name` as last_name,
											contact.`Contact ID` as lead_id,
											contact.`Account Name` as company,
											contact.Email as email,
											contact.`Mailing State/Province Code` as state,
											contact.`Contact ID` as contact_id,
											contact.`Account ID` as account_id,
											`Created Date` as created_date,
											`Lead Source` as lead_source,
											`Referrer Code` as referrer_code,
											`Owner ID` as owner_id
										FROM contact
									) as rollup
									LEFT JOIN opportunity opp ON opp.`Account ID` = account_id AND opp.`From Internal Account` = 0
										AND opp.`Opportunity Type` IN ('New Business', 'Opportunity')
									LEFT JOIN user ON rollup.owner_id = user.`User ID`
								WHERE
									created_date >= :start AND created_date <= :end
									$dynamicMetricWhere
									$dynamicBreakdownWhere
									$dynamicRepQuery
									AND opp.`Created Date` > created_date
									AND opp.`Opportunity ID` NOT IN (
										SELECT
											opp.`Opportunity ID`
										FROM (
												SELECT
													lead.`Converted Contact ID` as contact_id,
													lead.`Converted Account ID` as account_id,
													`Created Date` as created_date,
													`Lead Source` as lead_source,
													`Referrer Code` as referrer_code,
													`Owner ID` as owner_id
												FROM lead
												WHERE
													`Converted` = 0
												UNION
												SELECT
													contact.`Contact ID` as contact_id,
													contact.`Account ID` as account_id,
													`Created Date` as created_date,
													`Lead Source` as lead_source,
													`Referrer Code` as referrer_code,
													`Owner ID` as owner_id
												FROM contact
											) as rollup
											LEFT JOIN opportunity_contact_role opp_role ON rollup.contact_id = opp_role.`Contact ID`
											LEFT JOIN opportunity opp ON opp.`Opportunity ID` = opp_role.`Opportunity ID` AND opp.`From Internal Account` = 0
												AND opp.`Opportunity Type` IN ('New Business', 'Opportunity')
											LEFT JOIN user ON rollup.owner_id = user.`User ID`
										WHERE
											created_date >= :start2 AND created_date <= :end2
											$dynamicMetricWhere
											$dynamicBreakdownWhere
											$dynamicRepQuery
											AND opp.`Opportunity ID` IS NOT NULL
											ORDER BY rollup.created_date ASC
									)
								ORDER BY opp.`Opportunity ID`, lead_source, referrer_code, user.`User ID`
						");
						$stmt->execute(array(':start' => $_GET['start'], ':end' => $_GET['end'], ':start2' => $_GET['start'], ':end2' => $_GET['end']));
						$results2 = $stmt->fetchAll();

						$results = array_merge($results, $results2);
				}

/*
								`Referrer Code` as referrer_code,
								`Owner ID` as owner_id
							FROM contact
							WHERE
								`Converted Lead ID` IS NULL
						) as rollup
						LEFT JOIN opportunity_contact_role opp_role ON rollup.contact_id = opp_role.`Contact ID`
 */

				echo json_encode($results);

				break;
		}

		break;


	case 'leadAging':
		$leadAgingResults = array();

		$dates = array();

		if ($_GET['dateRollup'] == 'monthly') {
			for ($i=0; $i < 10; $i++) {

				if ($i == 0) {
					$query_date = date('Y-m-d');
				} else {
					$query_date = date('Y-m-01', strtotime('-' . $i . ' month'));
				}

				// First day of the month.
				// echo date('Y-m-01', strtotime($query_date));
				$start = date('Y-m-01', strtotime($query_date));

				// Last day of the month.
				// echo date('Y-m-t', strtotime($query_date));
				$end = date('Y-m-t', strtotime($query_date));
				//echo "\n\n";

				$dates[] = array('label' => date('M Y', strtotime($query_date)), 'start' => $start, 'end' => $end);
			}
		} else {
			$dates = array(
				array(
					'label' => 'Q4 2021',
					'start' => '2020-11-01',
					'end' => '2021-01-31',
				),
				array(
					'label' => 'Q3 2021',
					'start' => '2020-08-01',
					'end' => '2020-10-31',
				),
				array(
					'label' => 'Q2 2021',
					'start' => '2020-05-01',
					'end' => '2020-07-31',
				),
				array(
					'label' => 'Q1 2021',
					'start' => '2020-02-01',
					'end' => '2020-04-30',
				),
			);
		}

		// for ($i=0; $i < 3	; $i++) {
		foreach ($dates as $date){
			$tempOutput = array();

			// $startTemp = new DateTime();
			// $endDate = date('Y-m-01', strtotime('+1 month'));

			// if ($i == 0) {
			// 	$query_date = date('Y-m-d');
			// } else {
			// 	$query_date = date('Y-m-01', strtotime('-' . $i . ' month'));
			// }

			$leadSourceWhere = '';

			$leadSourcesOrig = explode(',', $_GET['leadSources']);

			$leadSources = implode("', '", $leadSourcesOrig);

			$leadSourceWhere = "lead_source IN ('$leadSources')";

			if (stristr($_GET['leadSources'], 'Pending')) {
				$leadSourceWhere = '(' . $leadSourceWhere . ' OR lead_source IS NULL)';
			}

			$breakdownSelect = '';
			$breakdownGroupBy = '';
			$breakdownOrderBy = '';
			$breakdownSelectNested = $breakdownSelect;
			$breakdownGroupByNested = $breakdownGroupBy;
			$breakdownOrderByNested = $breakdownOrderBy;

			if (isset($_GET['breakdown'])) {
				switch ($_GET['breakdown']) {
					case 'leadsource':
						$breakdownSelect = ', CASE WHEN lead_source IS NULL then \'PENDING\' else lead_source END AS lead_source';
						$breakdownSelectNested = $breakdownSelect;
						$breakdownGroupBy = 'GROUP BY lead_source';
						$breakdownOrderBy = 'ORDER BY lead_source';
						$breakdownSelectNested = $breakdownSelect;
						$breakdownGroupByNested = $breakdownGroupBy;
						$breakdownOrderByNested = $breakdownOrderBy;
						break;

					case 'leadsource-w-referrer':
						$breakdownSelect = ', CASE WHEN lead_source IS NULL then \'PENDING\' else lead_source END AS lead_source
											, CASE WHEN referrer_code IS NULL then \'PENDING\' else referrer_code END AS referrer_code';
						$breakdownGroupBy = 'GROUP BY lead_source, referrer_code';
						$breakdownOrderBy = 'ORDER BY lead_source, referrer_code';
						$breakdownSelectNested = $breakdownSelect;
						$breakdownGroupByNested = $breakdownGroupBy;
						$breakdownOrderByNested = $breakdownOrderBy;
						break;

					case 'rep':
						$breakdownSelect = ', CONCAT(user.`First Name`, " ",user.`Last Name`) as rep_name, user.`User ID` as rep_id';
						$breakdownSelectNested = ', CONCAT(first_name, " ",last_name) as rep_name, rep_id';
						$breakdownGroupBy = 'GROUP BY CONCAT(user.`First Name`, " ",user.`Last Name`), user.`User ID`';
						$breakdownGroupByNested = 'GROUP BY CONCAT(first_name, " ",last_name), rep_id';
						$breakdownOrderBy = 'ORDER BY CONCAT(user.`First Name`, " ",user.`Last Name`), user.`User ID`';
						$breakdownOrderByNested = 'ORDER BY CONCAT(first_name, " ",last_name), rep_id';
						break;
				}
			}

			// First day of the month.
			// echo date('Y-m-01', strtotime($query_date));
			$start = $date['start'];

			// Last day of the month.
			// echo date('Y-m-t', strtotime($query_date));
			$end = $date['end'];
			//echo "\n\n";

			$tempOutput['date'] = $date['label'];

			$stmt = $pdo->prepare("
				SELECT
					COUNT(*) as leads,
					SUM(case when mql_date IS NOT NULL then
						case when lead_source != 'Event' then 1 else 0 end
						else 0 end) AS mqls,
					SUM(case when sal_date IS NOT NULL then 1 else 0 end) AS sals,
					SUM(case when sql_date IS NOT NULL then 1 else 0 end) AS sqls
					$breakdownSelect
				FROM (
					SELECT
						'Lead' as type,
						`Lead ID` as id,
						`Created Date` as created_date,
						`Lead Source` as lead_source,
						`Referrer Code` as referrer_code,
						`MQL Date` as mql_date,
						`SAL Date` as sal_date,
						`SQL Date` as sql_date,
						`Owner ID` as owner_id
					FROM lead
					WHERE
						`Converted` = 0
					UNION
					SELECT
						'Contact' as type,
						`Contact ID` as id,
						`Created Date` as created_date,
						`Lead Source` as lead_source,
						`Referrer Code` as referrer_code,
						`MQL Date` as mql_date,
						`SAL Date` as sal_date,
						`SQL Date` as sql_date,
						`Owner ID` as owner_id
					FROM contact
				) as rollup
				INNER JOIN user ON rollup.owner_id = user.`User ID`
				WHERE
				 	$leadSourceWhere
					AND created_date >= :start AND created_date <= :end" . $eventsSQLLeadTable . "
				$breakdownGroupBy
				$breakdownOrderBy
			");

			/*$stmt = $pdo->prepare("
				SELECT COUNT(*) as leads,
				SUM(case when (lead.`MQL Date` IS NOT NULL and lead.`Lead Source` != 'Event' and lead.`Lead Source` != 'Webinar') then 1
					 else 0 end) AS mqls,
				SUM(case when lead.`SAL Date` IS NOT NULL then 1 else 0 end) AS sals,
				SUM(case when lead.`SQL Date` IS NOT NULL then 1 else 0 end) AS sqls
				 $breakdownSelect
				FROM lead
				INNER JOIN user ON lead.`Owner ID` = user.`User ID`
				WHERE
				 $leadSourceWhere
				AND lead.`Created Date` >= :start AND lead.`Created Date` <= :end" . $eventsSQLLeadTable . "
				$breakdownGroupBy
				$breakdownOrderBy ");*/
			$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
			$tofuResults = $stmt->fetchAll();


			//print_r("
			$stmt = $pdo->prepare("
				SELECT
					COUNT(*) as leads_bofu,
					SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then 1 else 0 end) AS pipeline,
					SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then Amount else 0 end) AS pipeline_value,
					SUM(case when `Stage` IN ('Closed Won') then 1 else 0 end) AS closed,
					SUM(case when `Stage` IN ('Closed Won') then Amount else 0 end) AS closed_value,
					SUM(case when `Stage` IN ('Closed Lost') then 1 else 0 end) AS closed_lost,
					SUM(case when `Stage` IN ('Closed Lost') then Amount else 0 end) AS closed_lost_value
					$breakdownSelectNested
				FROM
					(
						SELECT
							opp.`Opportunity ID`,
							MAX(Stage) as Stage,
							MAX(Amount) as Amount,
							lead_source,
							referrer_code,
							user.`User ID` as rep_id,
							user.`First Name` as first_name,
							user.`Last Name` as last_name
						FROM (
								SELECT
									lead.`Converted Contact ID` as contact_id,
									lead.`Converted Account ID` as account_id,
									`Created Date` as created_date,
									`Lead Source` as lead_source,
									`Referrer Code` as referrer_code,
									`Owner ID` as owner_id
								FROM lead
								WHERE
									`Converted` = 0
								UNION
								SELECT
									contact.`Contact ID` as contact_id,
									contact.`Account ID` as account_id,
									`Created Date` as created_date,
									`Lead Source` as lead_source,
									`Referrer Code` as referrer_code,
									`Owner ID` as owner_id
								FROM contact
							) as rollup
							LEFT JOIN opportunity_contact_role opp_role ON rollup.contact_id = opp_role.`Contact ID`
							LEFT JOIN opportunity opp ON opp.`Opportunity ID` = opp_role.`Opportunity ID` AND opp.`From Internal Account` = 0
								AND opp.`Opportunity Type` IN ('New Business', 'Opportunity')
							LEFT JOIN user ON rollup.owner_id = user.`User ID`
						WHERE
						 	$leadSourceWhere
							AND created_date >= :start AND created_date <= :end" . $eventsSQLLeadTable . "
							AND opp.`Opportunity ID` IS NOT NULL
						GROUP BY opp.`Opportunity ID`, lead_source, referrer_code, user.`User ID`, user.`First Name`, user.`Last Name`
						ORDER BY opp.`Opportunity ID`, lead_source, referrer_code, user.`User ID`, user.`First Name`, user.`Last Name`

					) as opps
				$breakdownGroupByNested
				$breakdownOrderByNested
				");
			$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
			$bofuResults = $stmt->fetchAll();


			if (isset($_GET['marketingInfluenced']) && $_GET['marketingInfluenced'] == 'true') {


				$stmt = $pdo->prepare("
					SELECT
						COUNT(*) as leads_bofu,
						SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then 1 else 0 end) AS pipeline,
						SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then Amount else 0 end) AS pipeline_value,
						SUM(case when `Stage` IN ('Closed Won') then 1 else 0 end) AS closed,
						SUM(case when `Stage` IN ('Closed Won') then Amount else 0 end) AS closed_value,
						SUM(case when `Stage` IN ('Closed Lost') then 1 else 0 end) AS closed_lost,
						SUM(case when `Stage` IN ('Closed Lost') then Amount else 0 end) AS closed_lost_value
						$breakdownSelect
					FROM
						(
							SELECT
								opp.`Opportunity ID`,
								MAX(Stage) as Stage,
								MAX(Amount) as Amount,
								lead_source,
								referrer_code,
								user.`User ID`
							FROM (
									SELECT
										lead.`Converted Contact ID` as contact_id,
										lead.`Converted Account ID` as account_id,
										`Created Date` as created_date,
										`Lead Source` as lead_source,
										`Referrer Code` as referrer_code,
										`Owner ID` as owner_id
									FROM lead
									WHERE
										`Converted` = 0
									UNION
									SELECT
										contact.`Contact ID` as contact_id,
										contact.`Account ID` as account_id,
										`Created Date` as created_date,
										`Lead Source` as lead_source,
										`Referrer Code` as referrer_code,
										`Owner ID` as owner_id
									FROM contact
								) as rollup
								LEFT JOIN opportunity opp ON opp.`Account ID` = account_id AND opp.`From Internal Account` = 0
									AND opp.`Opportunity Type` IN ('New Business', 'Opportunity')
								LEFT JOIN user ON rollup.owner_id = user.`User ID`
							WHERE
							 	$leadSourceWhere
								AND created_date >= :start AND created_date <= :end" . $eventsSQLLeadTable . "
								AND opp.`Opportunity ID` IS NOT NULL
								AND opp.`Created Date` > created_date
								AND opp.`Opportunity ID` NOT IN (
									SELECT
										opp.`Opportunity ID`
									FROM (
											SELECT
												lead.`Converted Contact ID` as contact_id,
												lead.`Converted Account ID` as account_id,
												`Created Date` as created_date,
												`Lead Source` as lead_source,
												`Referrer Code` as referrer_code,
												`Owner ID` as owner_id
											FROM lead
											WHERE
												`Converted` = 0
											UNION
											SELECT
												contact.`Contact ID` as contact_id,
												contact.`Account ID` as account_id,
												`Created Date` as created_date,
												`Lead Source` as lead_source,
												`Referrer Code` as referrer_code,
												`Owner ID` as owner_id
											FROM contact
										) as rollup
										LEFT JOIN opportunity_contact_role opp_role ON rollup.contact_id = opp_role.`Contact ID`
										LEFT JOIN opportunity opp ON opp.`Opportunity ID` = opp_role.`Opportunity ID` AND opp.`From Internal Account` = 0
											AND opp.`Opportunity Type` IN ('New Business', 'Opportunity')
										LEFT JOIN user ON rollup.owner_id = user.`User ID`
									WHERE
									 	$leadSourceWhere
										AND created_date >= :start2 AND created_date <= :end2" . $eventsSQLLeadTable . "
										AND opp.`Opportunity ID` IS NOT NULL
								)
							GROUP BY opp.`Opportunity ID`, lead_source, referrer_code, user.`User ID`
							ORDER BY opp.`Opportunity ID`, lead_source, referrer_code, user.`User ID`

						) as opps
					$breakdownGroupBy
					$breakdownOrderBy
					");

				/*
				echo "SELECT
						COUNT(*) as leads_bofu,
						SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then 1 else 0 end) AS pipeline,
						SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then Amount else 0 end) AS pipeline_value,
						SUM(case when `Stage` IN ('Closed Won') then 1 else 0 end) AS closed,
						SUM(case when `Stage` IN ('Closed Won') then Amount else 0 end) AS closed_value,
						SUM(case when `Stage` IN ('Closed Lost') then 1 else 0 end) AS closed_lost,
						SUM(case when `Stage` IN ('Closed Lost') then Amount else 0 end) AS closed_lost_value
						$breakdownSelect
					FROM
						(
							SELECT
								opp.`Opportunity ID`,
								MAX(Stage) as Stage,
								MAX(Amount) as Amount,
								lead_source,
								referrer_code,
								user.`User ID`
							FROM (
									SELECT
										lead.`Converted Contact ID` as contact_id,
										lead.`Converted Account ID` as account_id,
										`Created Date` as created_date,
										`Lead Source` as lead_source,
										`Referrer Code` as referrer_code,
										`Owner ID` as owner_id
									FROM lead
									UNION
									SELECT
										contact.`Contact ID` as contact_id,
										contact.`Account ID` as account_id,
										`Created Date` as created_date,
										`Lead Source` as lead_source,
										`Referrer Code` as referrer_code,
										`Owner ID` as owner_id
									FROM contact
									WHERE
										`Converted Lead ID` IS NULL
								) as rollup
								LEFT JOIN opportunity_contact_role opp_role ON rollup.contact_id = opp_role.`Contact ID`
								LEFT JOIN opportunity opp ON opp.`Opportunity ID` = opp_role.`Opportunity ID` AND opp.`From Internal Account` = 0
									AND opp.`Opportunity Type` IN ('New Business', 'Opportunity')
								LEFT JOIN user ON rollup.owner_id = user.`User ID`
							WHERE
							 	$leadSourceWhere
								AND created_date >= :start AND created_date <= :end" . $eventsSQLLeadTable . "
								AND opp.`Opportunity ID` IS NOT NULL
							GROUP BY opp.`Opportunity ID`, lead_source, referrer_code, user.`User ID`
							ORDER BY opp.`Opportunity ID`, lead_source, referrer_code, user.`User ID`

						) as opps
					$breakdownGroupBy
					$breakdownOrderBy"; exit();
					*/


				/*$stmt = $pdo->prepare("SELECT COUNT(*) as leads_bofu,
					SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then 1 else 0 end) AS pipeline,
					SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then opp.Amount else 0 end) AS pipeline_value,
					SUM(case when `Stage` IN ('Closed Won') then 1 else 0 end) AS closed,
					SUM(case when `Stage` IN ('Closed Won') then opp.Amount else 0 end) AS closed_value,
					SUM(case when `Stage` IN ('Closed Lost') then 1 else 0 end) AS closed_lost,
					SUM(case when `Stage` IN ('Closed Lost') then opp.Amount else 0 end) AS closed_lost_value
					 $breakdownSelect
					FROM lead
					LEFT JOIN opportunity_contact_role opp_role ON lead.`Converted Contact ID` = opp_role.`Contact ID`
					LEFT JOIN opportunity opp ON opp.`Opportunity ID` = opp_role.`Opportunity ID` AND opp.`From Internal Account` = 0
						AND opp.`Opportunity Type` IN ('New Business', 'Opportunity')
					LEFT JOIN user ON lead.`Owner ID` = user.`User ID`
					WHERE
					 $leadSourceWhere
					AND lead.`Created Date` >= :start AND lead.`Created Date` <= :end" . $eventsSQLLeadTable . "
					$breakdownGroupBy
					$breakdownOrderBy ");*/
				$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59', ':start2' => $start . ' 00:00:00', ':end2' => $end . ' 23:59:59'));
				$bofuResultsInfluenced = $stmt->fetchAll();

				// var_dump($bofuResults);

				// var_dump($bofuResultsInfluenced);

				foreach ($bofuResults[0] as $key => $value) {
					if (!empty($bofuResultsInfluenced[0][$key])) {
						$bofuResults[0][$key] += $bofuResultsInfluenced[0][$key];
					}
				}

				// var_dump($bofuResults);

				// echo "<br /><br /><br />";

				//exit();

			}

			// echo json_encode($tofuResults);
			// echo json_encode($bofuResults);
			// exit();

			foreach ($tofuResults as $key => $result) {

				if (isset($_GET['breakdown'])) {
					switch ($_GET['breakdown']) {
						case 'leadsource':
							foreach ($bofuResults as $bofuKey => $bofuResult) {

								// echo $bofuResult['lead_source'];
								// echo " --||-- " . $tofuResults[$key]['lead_source'] . "\n\n";
								if ($bofuResult['lead_source'] == $tofuResults[$key]['lead_source']) {
									$tofuResults[$key]['pipeline'] = $bofuResult['pipeline'];
									$tofuResults[$key]['pipeline_value'] = $bofuResult['pipeline_value'];
									$tofuResults[$key]['closed'] = $bofuResult['closed'];
									$tofuResults[$key]['closed_value'] = $bofuResult['closed_value'];
									$tofuResults[$key]['closed_lost'] = $bofuResult['closed_lost'];
									$tofuResults[$key]['closed_lost_value'] = $bofuResult['closed_lost_value'];
								}
							}
							break;

						case 'leadsource-w-referrer':
							foreach ($bofuResults as $bofuKey => $bofuResult) {
								if ($bofuResult['lead_source'] == $tofuResults[$key]['lead_source'] &&
									$bofuResult['referrer_code'] == $tofuResults[$key]['referrer_code']) {
									$tofuResults[$key]['pipeline'] = $bofuResult['pipeline'];
									$tofuResults[$key]['pipeline_value'] = $bofuResult['pipeline_value'];
									$tofuResults[$key]['closed'] = $bofuResult['closed'];
									$tofuResults[$key]['closed_value'] = $bofuResult['closed_value'];
									$tofuResults[$key]['closed_lost'] = $bofuResult['closed_lost'];
									$tofuResults[$key]['closed_lost_value'] = $bofuResult['closed_lost_value'];
								}
							}
							break;

						case 'rep':
							foreach ($bofuResults as $bofuKey => $bofuResult) {
								if ($bofuResult['rep_name'] == $tofuResults[$key]['rep_name']) {
									$tofuResults[$key]['pipeline'] = $bofuResult['pipeline'];
									$tofuResults[$key]['pipeline_value'] = $bofuResult['pipeline_value'];
									$tofuResults[$key]['closed'] = $bofuResult['closed'];
									$tofuResults[$key]['closed_value'] = $bofuResult['closed_value'];
									$tofuResults[$key]['closed_lost'] = $bofuResult['closed_lost'];
									$tofuResults[$key]['closed_lost_value'] = $bofuResult['closed_lost_value'];
								}
							}
							break;

						default:
							$tofuResults[$key]['pipeline'] = $bofuResults[$key]['pipeline'];
							$tofuResults[$key]['pipeline_value'] = $bofuResults[$key]['pipeline_value'];
							$tofuResults[$key]['closed'] = $bofuResults[$key]['closed'];
							$tofuResults[$key]['closed_value'] = $bofuResults[$key]['closed_value'];
							$tofuResults[$key]['closed_lost'] = $bofuResults[$key]['closed_lost'];
							$tofuResults[$key]['closed_lost_value'] = $bofuResults[$key]['closed_lost_value'];
							break;
					}
				} else {
					$tofuResults[$key]['pipeline'] = $bofuResults[$key]['pipeline'];
					$tofuResults[$key]['pipeline_value'] = $bofuResults[$key]['pipeline_value'];
					$tofuResults[$key]['closed'] = $bofuResults[$key]['closed'];
					$tofuResults[$key]['closed_value'] = $bofuResults[$key]['closed_value'];
					$tofuResults[$key]['closed_lost'] = $bofuResults[$key]['closed_lost'];
					$tofuResults[$key]['closed_lost_value'] = $bofuResults[$key]['closed_lost_value'];
				}

				if (!isset($tofuResults[$key]['pipeline'])) {
					$tofuResults[$key]['pipeline'] = 0;
					$tofuResults[$key]['pipeline_value'] = 0;
					$tofuResults[$key]['closed'] = 0;
					$tofuResults[$key]['closed_value'] = 0;
					$tofuResults[$key]['closed_lost'] = 0;
					$tofuResults[$key]['closed_lost_value'] = 0;
				}
			}

			$results = $tofuResults;



			// echo "SELECT COUNT(*) as leads,
			// 	SUM(case when lead.`MQL Date` IS NOT NULL then 1 else 0 end) AS mqls,
			// 	SUM(case when lead.`SAL Date` IS NOT NULL then 1 else 0 end) AS sals,
			// 	SUM(case when lead.`SQL Date` IS NOT NULL then 1 else 0 end) AS sqls,
			// 	SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then 1 else 0 end) AS pipeline,
			// 	SUM(case when `Stage` IN ('Closed Won') then 1 else 0 end) AS closed,
			// 	SUM(case when `Stage` IN ('Closed Lost') then 1 else 0 end) AS closed_lost
			// 	 $breakdownSelect
			// 	FROM lead
			// 	LEFT JOIN opportunity_contact_role opp_role ON lead.`Converted Contact ID` = opp_role.`Contact ID`
			// 	LEFT JOIN opportunity opp ON opp.`Opportunity ID` = opp_role.`Opportunity ID`
			// 	LEFT JOIN user ON lead.`Owner ID` = user.`User ID`
			// 	WHERE
			// 	 $leadSourceWhere
			// 	AND lead.`Created Date` >= :start AND lead.`Created Date` <= :end" . $eventsSQLLeadTable . "
			// 	$breakdownGroupBy
			// 	$breakdownOrderBy ";
			// 	exit();

			// echo json_encode($results);
			// exit();


			$stmt = $pdo->prepare("
				SELECT
					SUM(cost) as cost
				FROM
					marketing_cost
				WHERE
					`date` >= :start
					AND `date` <= :end;
			");

			$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
			$marketingCost = $stmt->fetch()['cost'];

			if (isset($_GET['breakdown']) && $_GET['breakdown'] !== 'none') {
				$rolloupRow = array('breakout' => 'rollup', 'lead_source' => 0);
				foreach ($results as $result) {
					foreach ($result as $key => $value) {
						if (!isset($rolloupRow[$key])) $rolloupRow[$key] = 0;

						if ($key != 'lead_source'
							&& $key != 'referrer_code'
							&& $key != 'rep_name'
							&& $key != 'rep_id') {
							$rolloupRow[$key] += $value;
						}
					}
				}

				array_unshift($results, $rolloupRow);

				foreach ($results as $row => $result) {
					switch ($_GET['breakdown']) {
						case 'leadsource':
							if ($result['lead_source'] === 0) {
								$results[$row]['label'] = $date['label'];
								$results[$row]['marketing_cost'] =  $marketingCost;
							} else {
								$results[$row]['label'] = '<span class="label">' . $results[$row]['lead_source'] . '</span>';
								$results[$row]['breakout'] = $results[$row]['lead_source'];
							}
							break;
						case 'leadsource-w-referrer':
							if ($result['lead_source'] === 0) {
								$results[$row]['label'] = $date['label'];
								$results[$row]['marketing_cost'] =  $marketingCost;
							} else {
								$results[$row]['label'] = '<span class="label"><small>' . $results[$row]['lead_source'] . '</small><br /> ' . $results[$row]['referrer_code'] . '</span>';
								$results[$row]['breakout'] = $results[$row]['lead_source'] . '||' . $results[$row]['referrer_code'];
							}
							break;

						case 'rep':
							if ($result['rep_name'] === 0) {
								$results[$row]['label'] = $date['label'];
								$results[$row]['marketing_cost'] =  $marketingCost;
							} else {
								$results[$row]['label'] = '<span class="label">' . $results[$row]['rep_name'] . '</span>';
								$results[$row]['breakout'] = $results[$row]['rep_id'];
							}
							break;
					}
					$results[$row]['start_date'] = $start . ' 00:00:00';
					$results[$row]['end_date'] = $end . ' 23:59:59';
				}
			} else {
				foreach ($results as $row => $result) {
					$results[$row]['label'] = $date['label'];
					$results[$row]['start_date'] = $start . ' 00:00:00';
					$results[$row]['end_date'] = $end . ' 23:59:59';
					$results[$row]['breakout'] = 'rollup';
					$results[$row]['marketing_cost'] =  $marketingCost;
				}
			}

			// echo json_encode($rolloupRow);
			// // echo json_encode($results);
			// exit();

			/*



			// echo json_encode($results);
			$tempOutput['leads'] = $results['leads'];
			$tempOutput['mqls'] = $results['mqls'];
			$tempOutput['sals'] = $results['sals'];
			$tempOutput['sqls'] = $results['sqls'];

			$stmt = $pdo->prepare("SELECT COUNT(*) as deals,
				SUM(case when `Stage` IN ('Qualification', 'Quoted', 'Upside', 'Commit') then 1 else 0 end) AS pipeline,
				SUM(case when `Stage` IN ('Closed Won') then 1 else 0 end) AS closed,
				SUM(case when `Stage` IN ('Closed Lost') then 1 else 0 end) AS closed_lost
				FROM lead
				LEFT JOIN opportunity_contact_role opp_role ON lead.`Converted Contact ID` = opp_role.`Contact ID`
				LEFT JOIN opportunity opp ON opp.`Opportunity ID` = opp_role.`Opportunity ID`
				WHERE (lead.`Lead Source` IN ('Webform', 'Webinar', 'Event', 'Hubspot') OR lead.`Lead Source` IS NULL)
				AND lead.`Created Date` >= :start AND lead.`Created Date` <= :end" . $eventsSQLLeadTable);
				// AND opp.MQL = 1
				// AND opp.`Opportunity Type` = 'New Business'
				// AND opp.`From Internal Account` = 0");
			$stmt->execute(array(':start' => $start . ' 00:00:00', ':end' => $end . ' 23:59:59'));
			$results = $stmt->fetch();

			$tempOutput['deals'] = $results['deals'];
			$tempOutput['pipeline'] = $results['pipeline'];
			$tempOutput['closed'] = $results['closed'];
			$tempOutput['closed_lost'] = $results['closed_lost'];


			*/

			// echo json_encode($results);
			$leadAgingResults[] = array(
				'date' => $date['label'],
				'rows' => $results,
			);
		}


		array_walk_recursive($leadAgingResults, "replaceNullValueWithZero");

		echo json_encode($leadAgingResults);


		break;



}

function replaceNullValueWithZero(&$value) {
    $value = $value === null ? 0 : $value;
}

function replaceNullValueWithEmptyString(&$value) {
    $value = $value === null ? "" : $value;
}

// function