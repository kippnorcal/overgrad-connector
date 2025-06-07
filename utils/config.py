"""
Here is an explanation of the keys for teh items in the list below:
    name: String - Name of the endpoint.
    gcs_folder: String - The folder where files will be saved on Google Cloud Storage.
    file_name_prefix: String - The prefix for the ndjson files that each record is stored in.
    fields: Set - List of fields to filter out any unexpected fields and to verify all of teh fields are present.
    has_university_id: Bool - Indicates if teh data contains University IDs
    date_filter: Bool - To indicate if the API endpoint can be filtered by date
    nested_fields: List - A list of nested field in the data. This is used to flatten the data. Not all have this.
    custom_field: Dict - Indicates there are nested fields in the data. This data will be parsed out.
        custom_field.field_name: String - Key of the custom field.
        custom_field.gcs_folder: String - The folder where the custom field files will be saved on Google Cloud Storage.
        custom_field.file_name_prefix: String - The prefix for the ndjson files that each custom field record is stored in.
        custom_field.fields: Set - List of fields to filter out any unexpected fields and to verify all of teh fields are present.
"""

OVERGRAD_ENDPOINT_CONFIGS = [
    {
        "name": "schools",
        "gcs_folder": "schools",
        "file_name_prefix": "school_",
        "has_university_id": False,
        "date_filter": False,
        "has_grad_year": False,
        "fields": {
            "id",
            "object",
            "name"
        },
    },
    {
        "name": "students",
        "gcs_folder": "students",
        "file_name_prefix": "student_",
        "has_university_id": False,
        "date_filter": True,
        "has_grad_year": True,
        "fields": {
            "id",
            "object",
            "created_at",
            "updated_at",
            "email",
            "first_name",
            "last_name",
            "external_student_id",
            "graduation_year",
            "school_id",
            "school_name",
            "assigned_counselor_id",
            "assigned_counselor_first_name",
            "assigned_counselor_last_name",
            "assigned_counselor_email",
            "academics_unweighted_gpa",
            "academics_weighted_gpa",
            "academics_projected_act",
            "academics_projected_sat",
            "academics_act_superscore",
            "cademics_sat_superscore",
            "academics_highest_act",
            "academics_highest_preact",
            "academics_highest_preact_8_9",
            "academics_highest_aspire_10",
            "academics_highest_aspire_9",
            "academics_highest_sat",
            "academics_highest_psat_nmsqt",
            "academics_highest_psat_10",
            "academics_highest_psat_8_9",
            "telephone",
            "address",
            "gender",
            "birth_date",
            "ethnicity",
            "family_income",
            "fafsa_completed",
            "student_aid_index",
            "maximum_family_contribution",
            "pell_grant",
            "post_high_school_plan",
            "first_generation",
            "fathers_education",
            "mothers_education",
            "parent1_education",
            "parent2_education",
            "awards",
            "extracurricular_activities",
            "interests",
            "target_grad_rate",
            "ideal_grad_rate",
        },
        "nested_fields": [
            "assigned_counselor",
            "academics",
            "school"
        ],
        "custom_field": {
            "field_name": "custom_field_values",
            "gcs_folder": "student_custom_fields",
            "file_name_prefix": "student_custom_field_",
            "fields": {
                "id",
                "custom_field_id",
                "value_type",
                "value",
            }
        }
    },
    {
        "name": "admissions",
        "gcs_folder": "admissions",
        "file_name_prefix": "admission_",
        "has_university_id": True,
        "date_filter": True,
        "has_grad_year": True,
        "fields": {
            "id",
            "object",
            "created_at",
            "updated_at",
            "student_id",
            "student_external_id",
            "university_id",
            "university_ipeds_id",
            "applied_on",
            "application_source",
            "due_date_date",
            "due_date_type",
            "status",
            "status_updated_at",
            "waitlisted",
            "deferred",
            "academic_fit",
            "probability_of_acceptance",
            "award_letter_status",
            "award_letter_tuition_and_fees",
            "award_letter_housing_and_meals",
            "award_letter_books_and_supplies",
            "award_letter_transportation",
            "award_letter_other_education_costs",
            "award_letter_grants_and_scholarships_from_school",
            "award_letter_federal_pell_grant",
            "award_letter_grants_from_state",
            "award_letter_other_scholarships",
            "award_letter_work_study",
            "award_letter_federal_perkins_loan",
            "award_letter_federal_direct_subsidized_loan",
            "award_letter_federal_direct_unsubsidized_loan",
            "award_letter_parent_plus_loan",
            "award_letter_military_benefits",
            "award_letter_private_loan",
            "award_letter_cost_of_attendance",
            "award_letter_grants_and_scholarships",
            "award_letter_net_cost",
            "award_letter_loans",
            "award_letter_other_options",
            "award_letter_out_of_pocket",
            "award_letter_unmet_need",
            "award_letter_unmet_need_with_max_family_contribution",
            "award_letter_seog",
        },
        "nested_fields": [
            "student",
            "university",
            "due_date",
            "award_letter"
        ],
        "custom_field": {
            "field_name": "custom_field_values",
            "gcs_folder": "admission_custom_fields",
            "file_name_prefix": "admission_custom_field_",
            "fields": {
                "id",
                "custom_field_id",
                "value_type",
                "value",
            }
        }
    },
    {
        "name": "followings",
        "gcs_folder": "followings",
        "file_name_prefix": "following_",
        "has_university_id": True,
        "date_filter": True,
        "has_grad_year": True,
        "fields": {
            "id",
            "object",
            "created_at",
            "updated_at",
            "student_id",
            "student_external_id",
            "university_id",
            "university_ipeds_id",
            "rank",
            "academic_fit",
            "probability_of_acceptance",
            "added_by"
        },
        "nested_fields": [
            "student",
            "university"
        ]
    },
    {
        "name": "universities",
        "gcs_folder": "universities",
        "file_name_prefix": "university_",
        "has_university_id": False,
        "date_filter": False,
        "has_grad_year": False,
        "fields": {
            "id",
            "object",
            "name",
            "ipeds_id",
            "status",
            "city",
            "state",
        },
    },
    {
        "name": "custom_fields",
        "gcs_folder": "custom_fields",
        "file_name_prefix": "custom_field_",
        "has_university_id": False,
        "date_filter": True,
        "has_grad_year": False,
        "fields": {
            "id",
            "object",
            "created_at",
            "updated_at",
            "name",
            "description",
            "resource_class",
            "field_type",
            "format",
            "student_can_view",
            "student_can_edit",
        },
        "custom_field": {
            "field_name": "custom_field_options",
            "gcs_folder": "custom_field_options",
            "file_name_prefix": "custom_field_",
            "fields": {
                "custom_field_id",
                "id",
                "object",
                "created_at",
                "updated_at",
                "custom_field_id",
                "label"
            }
        }
    }
]