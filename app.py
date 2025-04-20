from flask import Flask, request, jsonify
from flask_cors import CORS
import joblib
import numpy as np

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend-backend communication

# Load trained XGBoost model
model = joblib.load('loan_approval_model.pkl')

@app.route('/predict', methods=['POST'])
def predict():
    data = request.json

    # Extract features from incoming JSON payload
    features = [
        data['purchaser_type'],
        data['preapproval'],
        data['reverse_mortgage'],
        data['open_end_line_of_credit'],
        data['loan_amount'],
        data['loan_to_value_ratio'],
        data['interest_rate'],
        data['loan_term'],
        data['negative_amortization'],
        data['interest_only_payment'],
        data['balloon_payment'],
        data['other_nonamortizing_features'],
        data['property_value'],
        data['total_units'],
        data['income'],
        data['applicant_credit_score_type'],
        data['co_applicant_credit_score_type'],
        data['applicant_age_above_62'],
        data['co_applicant_age_above_62'],
        data['tract_population'],
        data['tract_minority_population_percent'],
        data['ffiec_msa_md_median_family_income'],
        data['tract_to_msa_income_percentage'],
        data['tract_owner_occupied_units'],
        data['tract_median_age_of_housing_units'],
        data['derived_loan_product_type_FHA_First_Lien'],
        data['derived_loan_product_type_FSA_RHS_First_Lien'],
        data['derived_loan_product_type_VA_First_Lien'],
        data['derived_dwelling_category_Multifamily_Site_Built'],
        data['derived_dwelling_category_SingleFamily_Manufactured'],
        data['loan_purpose_2'],
        data['loan_purpose_4'],
        data['loan_purpose_5'],
        data['loan_purpose_31'],
        data['loan_purpose_32'],
        data['occupancy_type_2'],
        data['occupancy_type_3'],
        data['submission_of_application_2'],
        data['initially_payable_to_institution_2'],
        data['aus_1_2'],
        data['aus_1_3'],
        data['aus_1_4'],
        data['aus_1_5'],
        data['aus_1_6'],
        data['aus_1_7']
    ]


    features_array = np.array(features).reshape(1, -1)

    # Model prediction
    prediction = model.predict(features_array)[0]
    probability = model.predict_proba(features_array).max()

    result = {
        "approval_status": int(prediction)
    }

    return jsonify(result)

@app.route('/', methods=['GET'])
def home():
    return "Loan Approval API is running!"

@app.route('/predict_interest_rate', methods=['POST'])
def predict_interest_rate():
    data = request.json

    # Only include the model-required fields:
    features = [
        data['loan_amount'],
        data['loan_term'],
        data['loan_purpose_2'],
        data['loan_purpose_4'],
        data['loan_purpose_5'],
        data['loan_type_2'],
        data['loan_type_3'],
        data['lien_status_2'],
        data['negative_amortization'],
        data['interest_only_payment'],
        data['balloon_payment'],
        data['other_nonamortizing_features'],
        data['conforming_loan_limit_NC'],
        data['derived_loan_product_type_Conventional:Subordinate Lien'],
        data['derived_loan_product_type_FHA:First Lien'],
        data['derived_loan_product_type_VA:First Lien'],
        data['derived_loan_product_type_VA:Subordinate Lien'],
        data['applicant_credit_score_type'],
        data['co-applicant_credit_score_type'],
        data['co-applicant_age'],
        data['applicant_age_above_62'],
        data['occupancy_type_3'],
        data['manufactured_home_secured_property_type_2'],
        data['manufactured_home_land_property_interest_3'],
        data['submission_of_application_3'],
        data['initially_payable_to_institution_3'],
        data['purchaser_type'],
        data['business_or_commercial_purpose'],
        data['open-end_line_of_credit'],
        data['reverse_mortgage']
    ]

    interest_model = joblib.load('interest_rate_model.pkl')
    prediction = interest_model.predict([features])[0]

    return jsonify({
        'predicted_interest_rate': float(prediction)
    })

@app.route('/predict_borrower_risk', methods=['POST'])
def predict_borrower_risk():
    data = request.json

    features = [
        data['debt_to_income_ratio'],
        data['loan_to_value_ratio'],
        data['interest_rate'],
        data['loan_amount'],
        data['rate_spread'],
        data['total_loan_costs'],
        data['origination_charges'],
        data['loan_term'],
        data['income'],
        data['property_value'],
        data['applicant_credit_score_type'],
        data['co_applicant_credit_score_type'],
        data['co_applicant_age'],
        data['applicant_age_above_62'],
        data['co_applicant_age_above_62'],
        data['loan_type_2'],
        data['loan_type_3'],
        data['loan_type_4'],
        data['loan_purpose_2'],
        data['loan_purpose_4'],
        data['loan_purpose_5'],
        data['loan_purpose_31'],
        data['loan_purpose_32'],
        data['derived_loan_product_type_Conventional:Subordinate_Lien'],
        data['derived_loan_product_type_FHA:First_Lien'],
        data['derived_loan_product_type_FHA:Subordinate_Lien'],
        data['derived_loan_product_type_FSA/RHS:First_Lien'],
        data['derived_loan_product_type_FSA/RHS:Subordinate_Lien'],
        data['derived_loan_product_type_VA:First_Lien'],
        data['derived_loan_product_type_VA:Subordinate_Lien'],
        data['occupancy_type_2'],
        data['occupancy_type_3'],
        data['derived_dwelling_category_Multifamily:Site-Built'],
        data['derived_dwelling_category_Single Family (1-4 Units):Manufactured'],
        data['derived_dwelling_category_Single Family (1-4 Units):Site-Built'],
        data['derived_msa-md'],
        data['tract_minority_population_percent'],
        data['ffiec_msa_md_median_family_income']
    ]

    # Load the appropriate model if not already loaded
    borrower_model = joblib.load('high_risk_model.pkl')
    prediction = borrower_model.predict([features])[0]
    confidence = borrower_model.predict_proba([features]).max()

    return jsonify({
        "risk_classification": int(prediction),
    })

@app.route('/predict_property_value', methods=['POST'])
def predict_property_value():
    property_value_model  = joblib.load('property_value_model.pkl')
    data = request.json

    # Adjust to match the features used in your model
    features = [
        int(data['purchaser_type']),
        int(data['preapproval']),
        int(data['reverse_mortgage']),
        int(data['open-end_line_of_credit']),
        int(data['business_or_commercial_purpose']),
        float(data['loan_amount']),
        float(data['loan_to_value_ratio']),
        float(data['interest_rate']),
        int(data['loan_term']),
        int(data['negative_amortization']),
        int(data['interest_only_payment']),
        int(data['balloon_payment']),
        int(data['other_nonamortizing_features']),
        int(data['total_units']),
        float(data['income']),
        int(data['applicant_credit_score_type']),
        int(data['co-applicant_credit_score_type']),
        int(data['co-applicant_age']),
        int(data['applicant_age_above_62']),
        int(data['co-applicant_age_above_62']),
        int(data['tract_population']),
        float(data['tract_minority_population_percent']),
        float(data['ffiec_msa_md_median_family_income']),
        float(data['tract_to_msa_income_percentage']),
        int(data['tract_owner_occupied_units']),
        int(data['tract_median_age_of_housing_units']),
        int(data['derived_loan_product_type_Conventional:Subordinate Lien']),
        int(data['derived_loan_product_type_FHA:First Lien']),
        int(data['derived_loan_product_type_FHA:Subordinate Lien']),
        int(data['derived_loan_product_type_FSA/RHS:First Lien']),
        int(data['derived_loan_product_type_FSA/RHS:Subordinate Lien']),
        int(data['derived_loan_product_type_VA:First Lien']),
        int(data['derived_loan_product_type_VA:Subordinate Lien']),
        int(data['derived_dwelling_category_Multifamily:Site-Built']),
        int(data['derived_dwelling_category_Single Family (1-4 Units):Manufactured']),
        int(data['conforming_loan_limit_NC']),
        int(data['conforming_loan_limit_U']),
        int(data['loan_purpose_2']),
        int(data['loan_purpose_4']),
        int(data['loan_purpose_5']),
        int(data['loan_purpose_31']),
        int(data['loan_purpose_32']),
        int(data['occupancy_type_2']),
        int(data['occupancy_type_3']),
        int(data['manufactured_home_secured_property_type_2']),
        int(data['manufactured_home_secured_property_type_1111']),
        int(data['manufactured_home_land_property_interest_2']),
        int(data['manufactured_home_land_property_interest_3']),
        int(data['manufactured_home_land_property_interest_4']),
        int(data['submission_of_application_2']),
        int(data['submission_of_application_1111']),
        int(data['initially_payable_to_institution_2'])
    ]
 # Predict and return the result
    prediction = property_value_model.predict([features])[0]
    return jsonify({
        'predicted_property_value': round(float(prediction), 2)
    })


if __name__ == '__main__':
    app.run(host='localhost', port=5000, debug=True)
