#!/usr/bin/env ruby

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
    0 => 'SELECT *, NOW() AS created_at, NOW() AS updated_at FROM countries ORDER BY created_at ASC;',
    1 => 'SELECT users.id, users.auth_token, people.birth_date as birthday, users.current_sign_in_at, users.current_sign_in_ip, users.email, users.encrypted_password, users.last_sign_in_at, users.last_sign_in_ip, users.passwd_login, people.phone_number AS phone, users.remember_created_at, users.reset_password_token, users.reset_password_sent_at, CASE WHEN users.role = "admin" THEN 1 WHEN users.role = "general" THEN 2 WHEN users.role = "doctor" THEN 3 WHEN users.role = "employee" THEN 4 WHEN users.role = "evaluator" THEN 5 ELSE NULL END AS role, users.sign_in_count, CASE WHEN people.neighbourhood_id > 0 THEN 3166 ELSE 3166 END AS country_id, users.created_at, users.updated_at FROM users, people WHERE people.user_id = users.id ORDER BY users.created_at ASC;',
    2 => 'SELECT people.id, people.last_name, people.name, people.is_male AS sex, users.stripe_id, users.stripe_subscription_id, people.user_id, people.created_at, people.updated_at FROM people, users WHERE people.user_id = users.id AND people.user_id IS NOT NULL ORDER BY people.created_at ASC;',
    3 => 'SELECT id, available, avatar, header, last_seen_at, minsa_number, self_description, signature, specialty, years_of_experience, user_id, created_at, updated_at FROM doctors WHERE user_id IS NOT NULL ORDER BY created_at ASC;',
    4 => 'SELECT telemedicines.id, telemedicines.hear_about_us, telemedicines.our_services_result, telemedicines.profesional_actions_result, telemedicines.profesional_conection_result, telemedicines.profesional_interaction_result, telemedicines.satisfaction_result, people.id AS person_id, telemedicines.created_at, telemedicines.updated_at FROM telemedicines, people, users WHERE telemedicines.user_id = users.id AND people.user_id = users.id ORDER BY telemedicines.created_at ASC;',
    5 => 'SELECT id, 0, person_id, created_at, updated_at FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL) ORDER BY created_at ASC;',
    6 => 'SELECT credit_users.id, credit_users.credit, people.id AS person_id, credit_users.created_at, credit_users.updated_at FROM credit_users, people, users WHERE credit_users.user_id = users.id AND people.user_id = users.id ORDER BY people.created_at ASC;',
    7 => 'SELECT id, finished_at, finished, specialty, taken, twilio_channel_name, twilio_channel_sid, CASE WHEN examination_type = "chat" THEN 1 WHEN examination_type = "video" THEN 2 WHEN examination_type = "expediente" THEN 3 WHEN examination_type = "NUTRIMIND_TEST_PLAN" THEN 4 ELSE NULL END AS type, doctor_id, person_id, created_at, updated_at FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL) ORDER BY created_at ASC;',
    8 => 'SELECT id, DATE(appointment_date) AS date, TIME(appointment_date) AS hour, reason, specialty, CASE WHEN status = "scheduled" THEN 1 WHEN status = "pending" THEN 2 WHEN status = "finished" THEN 3 ELSE NULL END AS status, taken, CASE WHEN ap_type = "plan" THEN 1 ELSE NULL END AS type, doctor_id, person_id, created_at, updated_at FROM appointments WHERE person_id IS NOT NULL AND doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL) ORDER BY created_at ASC;',
    9 => 'SELECT id, end_date, start_date, status, person_id, created_at, updated_at FROM memberships ORDER BY created_at ASC;',
    10 => 'SELECT CAST(@rownum := @rownum + 1 AS UNSIGNED) AS id, p.diag_diabetes AS diabetes, p.exercise, p.health_condition AS health, p.healthy_foods, p.diag_heart AS heart, p.highpressure, p.mental_health_condition AS mental_health, p.diag_obesity AS obesity, p.does_smoke AS smoke, p.id AS person_id, p.created_at, p.updated_at FROM people p, (SELECT @rownum := 0) t WHERE p.user_id IS NOT NULL ORDER BY created_at ASC;',
    11 => 'SELECT waist_widths.id, CASE WHEN body_mass_indices.cintura_cat = "Normal" THEN 1 WHEN body_mass_indices.cintura_cat = "Riesgo aumentado" THEN 2 WHEN body_mass_indices.cintura_cat = "Riesgo muy aumentado" THEN 3 ELSE NULL END AS category, waist_widths.risk_category AS risk, waist_widths.weight AS waist, waist_widths.session_id, waist_widths.created_at, waist_widths.updated_at FROM waist_widths, body_mass_indices WHERE body_mass_indices.session_id = waist_widths.session_id ORDER BY waist_widths.created_at ASC;',
    12 => 'SELECT id, height, imc, result, CASE WHEN risk_category = "Sin riesgo" THEN 0 WHEN risk_category = "Riesgo Bajo" THEN 1 WHEN risk_category = "Riesgo medio" THEN 2 WHEN risk_category = "Riesgo Alto" THEN 3 ELSE NULL END AS risk, weight, session_id, created_at, updated_at FROM body_mass_indices WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    13 => 'SELECT id, mind, risk_category AS risk, session_id, created_at, updated_at FROM emotional_states ORDER BY created_at ASC;',
    14 => 'SELECT id, health, risk_category AS risk, session_id, created_at, updated_at FROM health_states ORDER BY created_at ASC;',
    15 => 'SELECT id, comments, friday_breakfast, friday_dinner, friday_lunch, friday_snack_dinner, friday_snack_lunch, friday_snack, monday_breakfast, monday_dinner, monday_lunch, monday_snack_dinner, monday_snack_lunch, monday_snack, saturday_breakfast, saturday_dinner, saturday_lunch, saturday_snack_dinner, saturday_snack_lunch, saturday_snack, sunday_breakfast, sunday_dinner, sunday_lunch, sunday_snack_dinner, sunday_snack_lunch, sunday_snack, thursday_breakfast, thursday_dinner, thursday_lunch, thursday_snack_dinner, thursday_snack_lunch, thursday_snack, tuesday_breakfast, tuesday_dinner, tuesday_lunch, tuesday_snack_dinner, tuesday_snack_lunch, tuesday_snack, wednesday_breakfast, wednesday_dinner, wednesday_lunch, wednesday_snack_dinner, wednesday_snack_lunch, wednesday_snack, examination_id, created_at, updated_at FROM nutritional_plans WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    16 => 'SELECT id, plan, reason_attention AS reason, examination_id, created_at, updated_at FROM soaps WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    17 => 'SELECT id, actividad AS activity, anxiety_food, breakfast, culpability, diag_diabetes AS diabetes, eat_out_frecuency, frecuencia_ejercicio AS exercise_frequency, salud AS health, diag_heart AS heart, altura AS height, hungry_after_dinner_food AS hungry_after_dinner, imc, animo AS mind, non_pathological_background, situation, cintura AS waist, peso AS weight, examination_id, created_at, updated_at FROM nutrimind_tests WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;'
  }[queryIndex]
end

def getDf(index)
  Daru::DataFrame.new(@myclient.query(getQuery(index)).to_a)
end

def copyToDB(table)
  @pgconn.exec("COPY #{table} FROM '/tmp/#{table}.csv' DELIMITER ',' CSV HEADER;")
end

if __FILE__ == $PROGRAM_NAME
  tables = {
    0 => 'countries',
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
    puts table
    begin
      df = getDf(index)
      df.write_csv("/tmp/#{table}.csv")
      #puts `cp evital_production/#{table}.csv /tmp/.`
      copyToDB(table)
      File.delete("/tmp/#{table}.csv") if File.exist?("/tmp/#{table}.csv")
    rescue Exception => e
      puts e
      File.delete("/tmp/#{table}.csv") if File.exist?("/tmp/#{table}.csv")
      exit
    end
  end
end
