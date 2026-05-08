CREATE TABLE
  CodeNodes ( id STRING(MAX) NOT NULL,
    name STRING(MAX),
    type STRING(MAX),
    text STRING(MAX),
    embedding ARRAY<FLOAT32>(vector_length=>768),
    start_line INT64,
    end_line INT64,
    )
PRIMARY KEY
  (id);
CREATE VECTOR INDEX
  CodeEmbeddings
ON
  CodeNodes(embedding)
WHERE
  embedding IS NOT NULL OPTIONS ( distance_type = 'COSINE' );
CREATE TABLE
  DefinedIn ( code_id STRING(MAX) NOT NULL,
    file_name STRING(MAX) NOT NULL,
    )
PRIMARY KEY
  (code_id,
    file_name);
CREATE TABLE
  Dependencies ( name STRING(MAX) NOT NULL,
    )
PRIMARY KEY
  (name);
CREATE TABLE
  DependsOn ( code_id STRING(MAX) NOT NULL,
    dep_name STRING(MAX) NOT NULL,
    )
PRIMARY KEY
  (code_id,
    dep_name);
CREATE TABLE
  DocumentationNodes ( node_id STRING(MAX) NOT NULL,
    title STRING(MAX),
    SOURCE STRING(MAX),
    content STRING(MAX),
    embedding ARRAY<FLOAT32>(vector_length=>768),
    )
PRIMARY KEY
  (node_id);
CREATE VECTOR INDEX
  DocEmbeddings
ON
  DocumentationNodes(embedding)
WHERE
  embedding IS NOT NULL OPTIONS ( distance_type = 'COSINE' );
CREATE TABLE
  FILES ( name STRING(MAX) NOT NULL,
    )
PRIMARY KEY
  (name);
CREATE TABLE
  IssuePRMapping ( pr_number INT64 NOT NULL,
    issue_number INT64 NOT NULL,
    )
PRIMARY KEY
  (pr_number,
    issue_number);
CREATE TABLE
  Issues ( issue_number INT64 NOT NULL,
    graphql_id STRING(MAX),
    title STRING(MAX),
    author STRING(MAX),
    state STRING(MAX),
    assignees STRING(MAX),
    components STRING(MAX),
    statuses STRING(MAX),
    other_labels STRING(MAX),
    issue_type STRING(MAX),
    created_at STRING(MAX),
    updated_at STRING(MAX),
    closed_at STRING(MAX),
    body_text STRING(MAX),
    issue_summary STRING(MAX),
    environment_versions STRING(MAX),
    upvotes INT64,
    author_association STRING(MAX),
    embedding ARRAY<FLOAT32>(vector_length=>768),
    triage_category STRING(MAX),
    has_maintainer_resolution BOOL,
    resolution_snippet STRING(MAX),
    ai_duplicate_analysis STRING(MAX),
    verified_duplicate_ids STRING(MAX),
    )
PRIMARY KEY
  (issue_number);
CREATE VECTOR INDEX
  IssueEmbeddings
ON
  Issues(embedding)
WHERE
  embedding IS NOT NULL OPTIONS ( distance_type = 'COSINE' );
CREATE TABLE
  IssueComments ( issue_number INT64 NOT NULL,
    comment_id STRING(MAX) NOT NULL,
    author STRING(MAX),
    created_at STRING(MAX),
    )
PRIMARY KEY
  (issue_number,
    comment_id),
  INTERLEAVE IN PARENT Issues
ON
DELETE
  CASCADE;
CREATE TABLE
  PRModifiesCode ( pr_number INT64 NOT NULL,
    symbol_name STRING(MAX) NOT NULL,
    )
PRIMARY KEY
  (pr_number,
    symbol_name);
CREATE TABLE
  PullRequests ( pr_number INT64 NOT NULL,
    title STRING(MAX),
    author STRING(MAX),
    assignees STRING(MAX),
    reviewers STRING(MAX),
    state STRING(MAX),
    url STRING(MAX),
    pr_type STRING(MAX),
    created_at STRING(MAX),
    updated_at STRING(MAX),
    statuses STRING(MAX),
    body_text STRING(MAX),
    pr_summary STRING(MAX),
    diff_text STRING(MAX),
    ci_status STRING(MAX),
    review_decisions STRING(MAX),
    author_association STRING(MAX),
    upvotes INT64,
    embedding ARRAY<FLOAT32>(vector_length=>768),
    triage_category STRING(MAX),
    impacted_modules STRING(MAX),
    components STRING(MAX),
    other_labels STRING(MAX),
    ai_duplicate_analysis STRING(MAX),
    verified_duplicate_ids STRING(MAX),
    )
PRIMARY KEY
  (pr_number);
CREATE VECTOR INDEX
  PREmbeddings
ON
  PullRequests(embedding)
WHERE
  embedding IS NOT NULL OPTIONS ( distance_type = 'COSINE' );
CREATE TABLE
  PRComments ( pr_number INT64 NOT NULL,
    comment_id STRING(MAX) NOT NULL,
    author STRING(MAX),
    created_at STRING(MAX),
    )
PRIMARY KEY
  (pr_number,
    comment_id),
  INTERLEAVE IN PARENT PullRequests
ON
DELETE
  CASCADE;
CREATE TABLE
  PRDiffNodes ( pr_number INT64 NOT NULL,
    node_id STRING(MAX) NOT NULL,
    file_path STRING(MAX),
    diff_text STRING(MAX),
    embedding ARRAY<FLOAT32>(vector_length=>768),
    )
PRIMARY KEY
  (pr_number,
    node_id),
  INTERLEAVE IN PARENT PullRequests
ON
DELETE
  CASCADE;
CREATE VECTOR INDEX
  PRDiffEmbeddings
ON
  PRDiffNodes(embedding)
WHERE
  embedding IS NOT NULL OPTIONS ( distance_type = 'COSINE' );
CREATE TABLE
  ReleaseIncludesPR ( release_tag STRING(MAX) NOT NULL,
    pr_number INT64 NOT NULL,
    )
PRIMARY KEY
  (release_tag,
    pr_number);
CREATE TABLE
  Releases ( tag_name STRING(MAX) NOT NULL,
    name STRING(MAX),
    published_at STRING(MAX),
    body_text STRING(MAX),
    diff_text STRING(MAX),
    embedding ARRAY<FLOAT32>(vector_length=>768),
    )
PRIMARY KEY
  (tag_name);
CREATE VECTOR INDEX
  ReleaseEmbeddings
ON
  Releases(embedding)
WHERE
  embedding IS NOT NULL OPTIONS ( distance_type = 'COSINE' );
CREATE TABLE
  ReleaseFeatures ( tag_name STRING(MAX) NOT NULL,
    feature_id STRING(MAX) NOT NULL,
    description STRING(MAX),
    commit_hash STRING(MAX),
    diff_text STRING(MAX),
    summary STRING(MAX),
    embedding ARRAY<FLOAT32>(vector_length=>768),
    )
PRIMARY KEY
  (tag_name,
    feature_id),
  INTERLEAVE IN PARENT Releases
ON
DELETE
  CASCADE;
CREATE VECTOR INDEX
  ReleaseFeatureEmbeddings
ON
  ReleaseFeatures(embedding)
WHERE
  embedding IS NOT NULL OPTIONS ( distance_type = 'COSINE' );
CREATE TABLE
  SavedQueries ( query_name STRING(MAX) NOT NULL,
    description STRING(MAX),
    sql_query STRING(MAX),
    )
PRIMARY KEY
  (query_name);
CREATE TABLE
  SyncState ( job_name STRING(MAX) NOT NULL,
    last_value STRING(MAX) NOT NULL,
    )
PRIMARY KEY
  (job_name);
CREATE TABLE
  Users ( github_handle STRING(MAX) NOT NULL,
    company STRING(MAX),
    followers INT64,
    system_role STRING(MAX),
    last_updated STRING(MAX),
    )
PRIMARY KEY
  (github_handle); CREATE OR REPLACE PROPERTY GRAPH GraphRAG_Net
  NODE TABLES(
    CodeNodes
      KEY(id)
      LABEL CodeNodes PROPERTIES(
        embedding,
        end_line,
        id,
        name,
        start_line,
        text,
        type),

    Dependencies
      KEY(name)
      LABEL Dependencies PROPERTIES(
        name),

    Files
      KEY(name)
      LABEL Files PROPERTIES(
        name),

    Issues
      KEY(issue_number)
      LABEL Issues PROPERTIES(
        ai_duplicate_analysis,
        assignees,
        author,
        author_association,
        body_text,
        closed_at,
        components,
        created_at,
        embedding,
        environment_versions,
        graphql_id,
        has_maintainer_resolution,
        issue_number,
        issue_summary,
        issue_type,
        other_labels,
        resolution_snippet,
        state,
        statuses,
        title,
        triage_category,
        updated_at,
        upvotes,
        verified_duplicate_ids),

    PullRequests
      KEY(pr_number)
      LABEL PullRequests PROPERTIES(
        ai_duplicate_analysis,
        assignees,
        author,
        author_association,
        body_text,
        ci_status,
        components,
        created_at,
        diff_text,
        embedding,
        impacted_modules,
        other_labels,
        pr_number,
        pr_summary,
        pr_type,
        review_decisions,
        reviewers,
        state,
        statuses,
        title,
        triage_category,
        updated_at,
        upvotes,
        url,
        verified_duplicate_ids),

    Releases
      KEY(tag_name)
      LABEL Releases PROPERTIES(
        body_text,
        diff_text,
        embedding,
        name,
        published_at,
        tag_name)
  )
  EDGE TABLES(
    DefinedIn
      KEY(code_id, file_name)
      SOURCE KEY(code_id) REFERENCES CodeNodes(id)
      DESTINATION KEY(file_name) REFERENCES Files(name)
      LABEL DefinedIn PROPERTIES(
        code_id,
        file_name),

    DependsOn
      KEY(code_id, dep_name)
      SOURCE KEY(code_id) REFERENCES CodeNodes(id)
      DESTINATION KEY(dep_name) REFERENCES Dependencies(name)
      LABEL DependsOn PROPERTIES(
        code_id,
        dep_name),

    IssuePRMapping AS Resolves
      KEY(pr_number, issue_number)
      SOURCE KEY(pr_number) REFERENCES PullRequests(pr_number)
      DESTINATION KEY(issue_number) REFERENCES Issues(issue_number)
      LABEL Resolves PROPERTIES(
        issue_number,
        pr_number),

    ReleaseIncludesPR AS Ships
      KEY(release_tag, pr_number)
      SOURCE KEY(release_tag) REFERENCES Releases(tag_name)
      DESTINATION KEY(pr_number) REFERENCES PullRequests(pr_number)
      LABEL Ships PROPERTIES(
        pr_number,
        release_tag)
  );