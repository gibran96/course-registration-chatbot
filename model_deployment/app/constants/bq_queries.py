SIMILARITY_QUERY = """
    WITH query_embedding AS (
        SELECT ml_generate_embedding_result 
        FROM ML.GENERATE_EMBEDDING(
            MODEL `coursecompass.mlopsdataset.embeddings_model`,
            (SELECT @user_query AS content)
        )
    ),
    vector_search_results AS (
        SELECT 
            base.*,
            distance as search_distance
        FROM VECTOR_SEARCH(
            (
                SELECT *
                FROM `coursecompass.mlopsdataset.banner_data_embeddings`
                WHERE ARRAY_LENGTH(ml_generate_embedding_result) = 768
            ),
            'ml_generate_embedding_result',
            TABLE query_embedding,
            distance_type => 'COSINE',
            top_k => 5,
            options => '{"use_brute_force": true}'
        )
    ),
    course_matches AS (
        SELECT 
            v.*,
            c.crn AS course_crn
        FROM vector_search_results v
        JOIN `coursecompass.mlopsdataset.course_data_table` c
            ON v.faculty_name = c.instructor
    ),
    review_data AS (
        SELECT * EXCEPT(review_id)
        FROM `coursecompass.mlopsdataset.review_data_table`
    )
    SELECT DISTINCT
        cm.course_crn AS crn,
        cm.content,
        STRING_AGG(CONCAT(review.question, '\\n', review.response, '\\n'), '; ') AS concatenated_review_info,
        cm.search_distance AS score,
        CONCAT(
            'Course Information:\\n',
            cm.content,
            '\\nReview Information:\\n',
            STRING_AGG(CONCAT(review.question, '\\n', review.response, '\\n'), '; '),
            '\\n'
        ) AS full_info
    FROM course_matches cm
    JOIN review_data AS review
        ON cm.course_crn = review.crn
    GROUP BY
        cm.course_crn,
        cm.content,
        cm.search_distance
    """