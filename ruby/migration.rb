#!/usr/bin/env ruby
# frozen_string_literal: true

require 'daru'
require 'mysql2'
require 'pg'
require_relative '.credentials'

@myclient = Mysql2::Client.new(
  host: Credentials::MY_HOST,
  database: Credentials::MY_DB,
  username: Credentials::MY_USER,
  password: Credentials::MY_PASS
)

@pgconn = PG::Connection.open(
  dbname: Credentials::PG_DB,
  user: Credentials::PG_USER
)

def getQuery(queryIndex)
  {
    0 => 'WITH locations AS ( SELECT id, latitude, longitude, name, created_at, updated_at FROM  neighbourhoods ORDER BY created_at ASC ) SELECT * FROM locations ORDER BY created_at ASC;',
    1 => 'WITH users AS ( SELECT users.id, users.approved_terms_for_examinations, users.auth_token, people.birth_date as birthday, users.current_sign_in_at, users.current_sign_in_ip, users.email, users.encrypted_password, people.last_name, users.last_sign_in_at, users.last_sign_in_ip, people.name, users.passwd_login, users.people_filters, people.phone_number AS phone, people.phone_area_code, users.remember_created_at, users.reset_password_sent_at AS reset_sent_at, users.reset_password_token AS reset_token, users.role, people.is_male AS sex, users.sign_in_count, users.stripe_id, users.stripe_subscription_id, users.created_at, users.updated_at FROM users, people WHERE people.user_id = users.id ORDER BY users.created_at ASC ) SELECT * FROM users ORDER BY created_at ASC;',
    2 => 'SELECT id, has_children AS children, has_credit_card AS credit_card, private_insurance AS insurance, profession, race, neighbourhood_id AS localtion_id, user_id, created_at, updated_at FROM people ORDER BY created_at ASC;',
    3 => 'SELECT id, available, avatar, header, last_seen_at, minsa_number, self_description, signature, specialty, years_of_experience, user_id, created_at, updated_at FROM doctors ORDER BY created_at ASC;',
    4 => 'WITH telemedicines AS ( SELECT telemedicines.id, telemedicines.hear_about_us, telemedicines.our_services_result, telemedicines.profesional_actions_result, telemedicines.profesional_conection_result, telemedicines.profesional_interaction_result, telemedicines.satisfaction_result, people.id AS person_id, telemedicines.created_at, telemedicines.updated_at FROM telemedicines, people, users WHERE telemedicines.user_id = users.id AND people.user_id = users.id ORDER BY telemedicines.created_at ASC ) SELECT * FROM telemedicines ORDER BY created_at ASC;',
    5 => 'SELECT id, person_id, created_at, updated_at FROM sessions ORDER BY created_at ASC;',
    6 => 'WITH credits AS ( SELECT credit_users.id, credit_users.credit, people.id AS person_id, credit_users.created_at, credit_users.updated_at FROM credit_users, people, users WHERE credit_users.user_id = users.id AND people.user_id = users.id ORDER BY people.created_at ASC ) SELECT * FROM credits ORDER BY created_at ASC;',
    7 => 'SELECT id, finished_at, finished, specialty, taken, twilio_channel_name, twilio_channel_sid, service_type AS type, doctor_id, person_id, created_at, updated_at FROM examinations ORDER BY created_at ASC;',
    8 => 'SELECT id, appointment_date AS date, appointment_hour AS hour, reason, specialty, status, taken, ap_type AS type, person_id,doctor_id, created_at, updated_at FROM appointments ORDER BY created_at ASC;',
    9 => 'SELECT id, end_date, start_date, status, person_id, created_at, updated_at FROM memberships ORDER BY created_at ASC;',
    10 => 'WITH records AS ( SELECT ROW_NUMBER() OVER (ORDER BY people.id ASC) AS id, diag_diabetes AS diabetes, exercise, health_condition AS health, healthy_foods, diag_heart AS heart, highpressure, mental_health_condition AS mental_health, diag_obesity AS obesity, does_smoke AS smoke, id AS person_id, created_at, updated_at FROM people ORDER BY created_at ASC ) SELECT * FROM records ORDER BY created_at ASC;',
    11 => 'SELECT waist_widths.id, body_mass_indices.cintura_cat AS category, waist_widths.risk_category AS risk, waist_widths.weight AS waist, waist_widths.session_id, waist_widths.created_at, waist_widths.updated_at FROM waist_widths, body_mass_indices WHERE body_mass_indices.session_id = waist_widths.session_id ORDER BY waist_widths.created_at ASC;',
    12 => 'SELECT id, height, imc, result, risk_category AS risk, weight, session_id, created_at, updated_at FROM body_mass_indices ORDER BY created_at ASC;',
    13 => 'SELECT id, mind, risk_category AS risk, session_id, created_at, updated_at FROM emotional_states ORDER BY created_at ASC;',
    14 => 'SELECT id, health, risk_category AS risk, session_id, created_at, updated_at FROM health_states ORDER BY created_at ASC;',
    15 => 'SELECT id, comments, friday_breakfast, friday_dinner, friday_lunch, friday_snack_dinner, friday_snack_lunch, friday_snack, monday_breakfast, monday_dinner, monday_lunch, monday_snack_dinner, monday_snack_lunch, monday_snack, saturday_breakfast, saturday_dinner, saturday_lunch, saturday_snack_dinner, saturday_snack_lunch, saturday_snack, sunday_breakfast, sunday_dinner, sunday_lunch, sunday_snack_dinner, sunday_snack_lunch, sunday_snack, thursday_breakfast, thursday_dinner, thursday_lunch, thursday_snack_dinner, thursday_snack_lunch, thursday_snack, tuesday_breakfast, tuesday_dinner, tuesday_lunch, tuesday_snack_dinner, tuesday_snack_lunch, tuesday_snack, wednesday_breakfast, wednesday_dinner, wednesday_lunch, wednesday_snack_dinner, wednesday_snack_lunch, wednesday_snack, examination_id, created_at, updated_at FROM nutritional_plans ORDER BY created_at ASC;',
    16 => 'SELECT id, plan, reason_attention AS reason, examination_id, created_at, updated_at FROM soaps ORDER BY created_at ASC;',
    17 => 'SELECT id, actividad AS activity, anxiety_food, breakfast, culpability, diag_diabetes AS diabetes, eat_out_frecuency, frecuencia_ejercicio AS exercise_frequency, salud AS health, diag_heart AS heart, altura AS height, hungry_after_dinner_food AS hungry_after_dinner, imc, animo AS mind, non_pathological_background, situation, cintura AS waist, peso AS weight, created_at, updated_at FROM nutrimind_tests ORDER BY created_at ASC;'
  }[queryIndex]
end

def getDf(index)
  Daru::DataFrame.new(@myclient.query(getQuery(index)).to_a)
end

def copyToDB(table)
  @pgconn.exec("COPY #{table} FROM '#{table}.csv' DELIMITER ", ' CSV HEADER;')
end

if __FILE__ == $PROGRAM_NAME
  tables = {
    0 => 'locations',
    1 => 'users',
    2 => 'people',
    3 => 'doctors',
    4 => 'telemedicines',
    5 => 'sessions',
    6 => 'credits',
    7 => 'examinations',
    8 => 'appointments',
    9 => 'memberships',
    10 => 'records',
    11 => 'waist_widths',
    12 => 'body_mass_indices',
    13 => 'emotional_states',
    14 => 'health_states',
    15 => 'nutritional_plans',
    16 => 'soaps',
    17 => 'nutrimind_tests'
  }

  tables.each do |index, table|
    df = getDf(index)
    df.write_csv("#{table}.csv")
    copyToDB(table)
    File.delete("#{table}.csv") if File.exist?("#{table}.csv")
  end
end
