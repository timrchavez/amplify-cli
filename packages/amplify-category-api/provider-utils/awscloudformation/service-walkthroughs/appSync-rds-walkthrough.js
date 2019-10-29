const inquirer = require('inquirer');
const ora = require('ora');
const { DataApiParams } = require('graphql-relational-schema-transformer');

const spinner = ora('');
const category = 'api';
const providerName = 'awscloudformation';

async function serviceWalkthrough(context, defaultValuesFilename, datasourceMetadata) {
  const amplifyMeta = context.amplify.getProjectMeta();

  // Verify that an API exists in the project before proceeding.
  if (amplifyMeta == null || amplifyMeta[category] == null || Object.keys(amplifyMeta[category]).length === 0) {
    context.print.error(
      'You must create an AppSync API in your project before adding a graphql datasource. Please use "amplify api add" to create the API.'
    );
    process.exit(0);
  }

  // Loop through to find the AppSync API Resource Name
  let appSyncApi;
  const apis = Object.keys(amplifyMeta[category]);

  for (let i = 0; i < apis.length; i += 1) {
    if (amplifyMeta[category][apis[i]].service === 'AppSync') {
      appSyncApi = apis[i];
      break;
    }
  }

  // If an AppSync API does not exist, inform the user to create the AppSync API
  if (!appSyncApi) {
    context.print.error(
      'You must create an AppSync API in your project before adding a graphql datasource. Please use "amplify api add" to create the API.'
    );
    process.exit(0);
  }

  const { inputs, availableRegions } = datasourceMetadata;

  // Region Question
  const selectedRegion = await promptWalkthroughQuestion(inputs, 0, availableRegions);

  const AWS = await getAwsClient(context, 'list');

  // Prepare the SDK with the region
  AWS.config.update({
    region: selectedRegion,
  });

  // RDS Cluster Question
  const { selectedClusterArn, clusterResourceId, selectedClusterEngine } = await selectCluster(context, inputs, AWS);

  // Secret Store Question
  const selectedSecretArn = await getSecretStoreArn(context, inputs, clusterResourceId, AWS);

  // Database Name Question
  const selectedDatabase = await selectDatabase(context, inputs, selectedClusterArn, selectedClusterEngine, selectedSecretArn, AWS);

  const answers = {
    region: selectedRegion,
    dbClusterArn: selectedClusterArn,
    secretStoreArn: selectedSecretArn,
    databaseName: selectedDatabase,
    databaseEngine: selectedClusterEngine,
    resourceName: appSyncApi,
    databaseSchemas: [],
  };

  // PostgreSQL further partitions databases into namespaces called "schemas"
  if (selectedClusterEngine === 'aurora-postgresql') {
    // Scheme Name Question
    // FIXME(timrc): Support defaults (e.g. default to 'public')
    const selectedSchemas = await selectSchemas(context, inputs, selectedClusterArn, selectedSecretArn, selectedDatabase, AWS);
    answers.databaseSchemas = selectedSchemas;
  }

  return answers;
}

/**
 *
 * @param {*} inputs
 */
async function selectCluster(context, inputs, AWS) {
  const RDS = new AWS.RDS();

  const describeDBClustersResult = await RDS.describeDBClusters().promise();
  const rawClusters = describeDBClustersResult.DBClusters;
  const clusters = new Map();

  for (let i = 0; i < rawClusters.length; i += 1) {
    if (rawClusters[i].EngineMode === 'serverless') {
      clusters.set(rawClusters[i].DBClusterIdentifier, rawClusters[i]);
    }
  }

  if (clusters.size > 0) {
    const clusterIdentifier = await promptWalkthroughQuestion(inputs, 1, Array.from(clusters.keys()));
    const selectedCluster = clusters.get(clusterIdentifier);
    return {
      selectedClusterArn: selectedCluster.DBClusterArn,
      clusterResourceId: selectedCluster.DbClusterResourceId,
      selectedClusterEngine: selectedCluster.Engine,
    };
  }
  context.print.error('No properly configured Aurora Serverless clusters found.');
  process.exit(0);
}

/**
 *
 * @param {*} inputs
 * @param {*} clusterResourceId
 */
async function getSecretStoreArn(context, inputs, clusterResourceId, AWS) {
  const SecretsManager = new AWS.SecretsManager();
  const NextToken = 'NextToken';
  let rawSecrets = [];
  const params = {
    MaxResults: 20,
  };

  const listSecretsResult = await SecretsManager.listSecrets(params).promise();

  rawSecrets = listSecretsResult.SecretList;
  let token = listSecretsResult.NextToken;
  while (token) {
    params[NextToken] = token;
    const tempSecretsResult = await SecretsManager.listSecrets(params).promise();
    rawSecrets = [...rawSecrets, ...tempSecretsResult.SecretList];
    token = tempSecretsResult.NextToken;
  }

  const secrets = new Map();
  let selectedSecretArn;

  for (let i = 0; i < rawSecrets.length; i += 1) {
    /**
     * Attempt to auto-detect Secret Store that was created by Aurora Serverless
     * as it follows a specfic format for the Secret Name
     */
    if (rawSecrets[i].Name.startsWith(`rds-db-credentials/${clusterResourceId}`)) {
      // Found the secret store - store the details and break out.
      selectedSecretArn = rawSecrets[i].ARN;
      break;
    }
    secrets.set(rawSecrets[i].Name, rawSecrets[i].ARN);
  }

  if (!selectedSecretArn) {
    if (secrets.size > 0) {
      // Kick off questions flow
      const selectedSecretName = await promptWalkthroughQuestion(inputs, 2, Array.from(secrets.keys()));
      selectedSecretArn = secrets.get(selectedSecretName);
    } else {
      context.print.error('No RDS access credentials found in the AWS Secrect Manager.');
      process.exit(0);
    }
  }

  return selectedSecretArn;
}

function listDatabasesSql(clusterEngine) {
  if (clusterEngine === 'aurora-postgresql') {
    return 'SELECT datname FROM pg_database WHERE datistemplate = false';
  }
  return 'SHOW databases';
}

/**
 *
 * @param {*} inputs
 * @param {*} clusterArn
 * @param {*} secretArn
 */
async function selectDatabase(context, inputs, clusterArn, clusterEngine, secretArn, AWS) {
  // Database Name Question
  const DataApi = new AWS.RDSDataService();
  const params = new DataApiParams();
  const ignoreDatabases = {
    'aurora-postgresql': ['rdsadmin', 'postgres'],
    'aurora-mysql': ['information_schema', 'performance_schema', 'mysql'],
  };
  params.secretArn = secretArn;
  params.resourceArn = clusterArn;
  params.sql = listDatabasesSql(clusterEngine);

  spinner.start('Fetching Aurora Serverless cluster...');

  const dataApiResult = await DataApi.executeStatement(params).promise();

  // eslint-disable-next-line prefer-destructuring
  const records = dataApiResult.records;
  const databaseList = [];

  for (let i = 0; i < records.length; i += 1) {
    const recordValue = records[i][0].stringValue;
    // ignore the three meta tables that the cluster creates
    if (!ignoreDatabases[clusterEngine].includes(recordValue)) {
      databaseList.push(recordValue);
    }
  }

  spinner.succeed('Fetched Aurora Serverless cluster.');

  if (databaseList.length > 0) {
    return await promptWalkthroughQuestion(inputs, 3, databaseList);
  }

  context.print.error('No properly configured databases found.');
  process.exit(0);
}

function listSchemasSql() {
  return 'SELECT schema_name FROM information_schema.schemata;';
}

/**
 *
 * @param {*} inputs
 * @param {*} clusterArn
 * @param {*} secretArn
 * @param {*} database
 */
async function selectSchemas(context, inputs, clusterArn, secretArn, database, AWS) {
  // Database Name Question
  const DataApi = new AWS.RDSDataService();
  const params = new DataApiParams();
  params.secretArn = secretArn;
  params.resourceArn = clusterArn;
  params.database = database;
  params.sql = listSchemasSql();

  spinner.start('Fetching database schemas...');
  const dataApiResult = await DataApi.executeStatement(params).promise();

  // eslint-disable-next-line prefer-destructuring
  const records = dataApiResult.records;
  const schemaList = [];

  for (let i = 0; i < records.length; i += 1) {
    const recordValue = records[i][0].stringValue;
    // ignore these schemas that the cluster creates
    if (!['information_schema', 'pg_catalog'].includes(recordValue)) {
      schemaList.push(recordValue);
    }
  }

  spinner.succeed('Fetched database schemas.');

  if (schemaList.length > 0) {
    return await promptWalkthroughQuestion(inputs, 4, schemaList);
  }

  context.print.error('No valid database schemas found.');
  process.exit(0);
}

/**
 *
 * @param {*} inputs
 * @param {*} questionNumber
 * @param {*} choicesList
 * @param {*} defaultsList
 */
async function promptWalkthroughQuestion(inputs, questionNumber, choicesList) {
  let defaultsList = [];
  if ('selectFirst' in inputs[questionNumber] && inputs[questionNumber].selectFirst && choicesList.length > 0) {
    defaultsList = [choicesList[0]];
  }
  const question = [
    {
      type: inputs[questionNumber].type,
      name: inputs[questionNumber].key,
      message: inputs[questionNumber].question,
      choices: choicesList,
      default: defaultsList,
    },
  ];

  const answer = await inquirer.prompt(question);

  return answer[inputs[questionNumber].key];
}

async function getAwsClient(context, action) {
  const providerPlugins = context.amplify.getProviderPlugins(context);
  const provider = require(providerPlugins[providerName]);
  return await provider.getConfiguredAWSClient(context, 'aurora-serverless', action);
}

module.exports = {
  serviceWalkthrough,
};
