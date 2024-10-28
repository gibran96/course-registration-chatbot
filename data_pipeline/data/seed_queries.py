# Define the query templates with placeholders
query_templates = {
    "courses_offered": "What courses are being offered for the {semester} semester for the {program_name} program?",
    "professor_for_course": "Which professor is taking the course {course_name}?",
    "recommended_courses": "Based on my {program_name} program, what courses do you recommend for the {semester} semester?",
    "related_courses": "What are the courses available related to {topic}?",
    "courses_on_day": "Can you suggest courses that take place on {day_of_week}?",
    "evening_classes": "Can you suggest classes only after {time}?",
    "online_courses": "Can you please suggest online courses to take?",
    "research_courses": "Can you suggest research or seminar-based courses?",
    "no_exam_courses": "Can you suggest courses without exams?",
    "easy_courses": "Can you suggest courses which are not too hectic and easy to get good grades?",
    "class_timings": "What are the class timings for the course {course_name}?",
    "prerequisite_courses": "Are there any prerequisite courses for {course_name}?",
    "course_reviews": "Has the course {course_name} been offered in the past? If so, could you summarize the reviews?",
    "courses_with_resources": "Suggest a course with good reading materials (resources from the professor).",
    "courses_by_professor": "What courses are being offered by Professor {professor_name}?"
}

# Function to format and print each query
def generate_queries(**kwargs):
    for key, template in query_templates.items():
        try:
            # Attempt to format the template with the provided kwargs
            query = template.format(**kwargs)
            print(f"{key}: {query}\n")
        except KeyError:
            # If missing a keyword argument for a specific query, skip it
            print(f"{key}: Missing required placeholders for this query.\n")

