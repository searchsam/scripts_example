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
    1 => 'SELECT users.id, users.auth_token, people.birth_date as birthday, users.current_sign_in_at, users.current_sign_in_ip, "", users.email, users.encrypted_password, users.last_sign_in_at, users.last_sign_in_ip, users.passwd_login, people.phone_number AS phone, users.remember_created_at,  users.reset_password_sent_at, users.reset_password_token, CASE WHEN users.role = "admin" THEN 1 WHEN users.role = "general" THEN 2 WHEN users.role = "doctor" THEN 3 WHEN users.role = "employee" THEN 4 WHEN users.role = "evaluator" THEN 5 ELSE NULL END AS role, users.sign_in_count, CASE WHEN people.neighbourhood_id > 0 THEN 3166 ELSE 3166 END AS country_id, users.created_at, users.updated_at FROM users, people WHERE people.user_id = users.id ORDER BY users.created_at ASC;',
    2 => 'SELECT people.id, people.last_name, people.name, CASE WHEN people.is_male = 1 THEN TRUE WHEN people.is_male = 0 THEN FALSE ELSE NULL END AS sex, users.stripe_id, users.stripe_subscription_id, people.user_id, people.created_at, people.updated_at FROM people, users WHERE people.user_id = users.id AND people.user_id IS NOT NULL ORDER BY people.created_at ASC;',
    3 => 'SELECT id, available, avatar, header, last_seen_at, minsa_number, self_description, signature, CASE WHEN specialty = "General" THEN 1 WHEN specialty = "Medico General" THEN 1 WHEN specialty = "Nutricionista" THEN 2 ELSE NULL END AS specialty, years_of_experience, user_id, created_at, updated_at FROM doctors WHERE user_id IS NOT NULL ORDER BY created_at ASC;',
    4 => 'SELECT telemedicines.id, telemedicines.hear_about_us, telemedicines.our_services_result, telemedicines.profesional_actions_result, telemedicines.profesional_conection_result, telemedicines.profesional_interaction_result, telemedicines.satisfaction_result, people.id AS person_id, telemedicines.created_at, telemedicines.updated_at FROM telemedicines, people, users WHERE telemedicines.user_id = users.id AND people.user_id = users.id ORDER BY telemedicines.created_at ASC;',
    5 => 'SELECT sessions.id, 0, sessions.person_id, sessions.created_at, sessions.updated_at FROM sessions WHERE sessions.person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL) ORDER BY sessions.created_at ASC;',
    6 => 'SELECT credit_users.id, credit_users.credit, people.id AS person_id, credit_users.created_at, credit_users.updated_at FROM credit_users, people, users WHERE credit_users.user_id = users.id AND people.user_id = users.id ORDER BY people.created_at ASC;',
    7 => 'SELECT id, CASE WHEN examination_type = "chat" THEN 1 WHEN examination_type = "video" THEN 2 WHEN examination_type = "expediente" THEN 3 WHEN examination_type = "NUTRIMIND_TEST_PLAN" THEN 4 ELSE NULL END AS examination_type, finished_at, finished, CASE WHEN service_type = "paid" THEN 1 WHEN service_type = "free" THEN 2 ELSE 0 END AS service, CASE WHEN specialty = "Medico General" THEN 1 WHEN specialty = "Nutricionista" THEN 2 WHEN specialty = "NUTRIMIND_TEST_PLAN" THEN 4 ELSE NULL END AS specialty, taken, twilio_channel_name, twilio_channel_sid, doctor_id, person_id, created_at, updated_at FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL) ORDER BY created_at ASC;',
    8 => 'SELECT id, DATE(appointment_date) AS date, TIME(appointment_date) AS hour, reason, CASE WHEN specialty = "Medico General" THEN 1 WHEN specialty = "Nutricionista" THEN 2 WHEN specialty = "Psicología" THEN 3 WHEN specialty = "Plan nutricional" THEN 4 ELSE NULL END AS specialty, CASE WHEN status = "scheduled" THEN 1 WHEN status = "pending" THEN 2 WHEN status = "finished" THEN 3 ELSE NULL END AS status, taken, CASE WHEN ap_type = "plan" THEN 1 ELSE NULL END AS type, doctor_id, person_id, created_at, updated_at FROM appointments WHERE person_id IS NOT NULL AND doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL) ORDER BY created_at ASC;',
    9 => 'SELECT id, end_date, start_date, status, person_id, created_at, updated_at FROM memberships ORDER BY created_at ASC;',
    10 => 'SELECT CAST(@rownum := @rownum + 1 AS UNSIGNED) AS id, p.diag_diabetes AS diabetes, p.exercise, p.health_condition AS health, p.healthy_foods, p.diag_heart AS heart, p.highpressure, p.mental_health_condition AS mental_health, p.diag_obesity AS obesity, p.does_smoke AS smoke, p.id AS person_id, p.created_at, p.updated_at FROM people p, (SELECT @rownum := 0) t WHERE p.user_id IS NOT NULL ORDER BY created_at ASC;',
    11 => 'SELECT waist_widths.id, CASE WHEN body_mass_indices.cintura_cat = "Normal" THEN 1 WHEN body_mass_indices.cintura_cat = "Riesgo aumentado" THEN 2 WHEN body_mass_indices.cintura_cat = "Riesgo muy aumentado" THEN 3 ELSE NULL END AS category, waist_widths.risk_category AS risk, waist_widths.weight AS waist, waist_widths.session_id, waist_widths.created_at, waist_widths.updated_at FROM waist_widths, body_mass_indices WHERE body_mass_indices.session_id = waist_widths.session_id ORDER BY waist_widths.created_at ASC;',
    12 => 'SELECT id, height, imc, CASE WHEN result = "Infrapeso" THEN 1 WHEN result = "Peso Normal" THEN 2 WHEN result = "Sobrepeso" THEN 3 WHEN result = "Obesidad" THEN 4 ELSE NULL END AS category, CASE WHEN risk_category = "1" THEN 1 WHEN risk_category = "2" THEN 2 WHEN risk_category = "3" THEN 3 ELSE NULL END AS risk, weight, session_id, created_at, updated_at FROM body_mass_indices WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    13 => 'SELECT id, CAST(mind AS INT) AS mind, risk_category AS risk, session_id, created_at, updated_at FROM emotional_states ORDER BY created_at ASC;',
    14 => 'SELECT id, CAST(health AS INT) AS health, risk_category AS risk, session_id, created_at, updated_at FROM health_states ORDER BY created_at ASC;',
    15 => 'SELECT id, comments, friday_breakfast, friday_dinner, friday_lunch, friday_snack_dinner, friday_snack_lunch, friday_snack, monday_breakfast, monday_dinner, monday_lunch, monday_snack_dinner, monday_snack_lunch, monday_snack, saturday_breakfast, saturday_dinner, saturday_lunch, saturday_snack_dinner, saturday_snack_lunch, saturday_snack, sunday_breakfast, sunday_dinner, sunday_lunch, sunday_snack_dinner, sunday_snack_lunch, sunday_snack, thursday_breakfast, thursday_dinner, thursday_lunch, thursday_snack_dinner, thursday_snack_lunch, thursday_snack, tuesday_breakfast, tuesday_dinner, tuesday_lunch, tuesday_snack_dinner, tuesday_snack_lunch, tuesday_snack, wednesday_breakfast, wednesday_dinner, wednesday_lunch, wednesday_snack_dinner, wednesday_snack_lunch, wednesday_snack, examination_id, created_at, updated_at FROM nutritional_plans WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    16 => 'SELECT id, plan, reason_attention AS reason, examination_id, created_at, updated_at FROM soaps WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    17 => 'SELECT id, actividad AS activity, anxiety_food, culpability, diag_diabetes AS diabetes, eat_out_frecuency, frecuencia_ejercicio AS exercise_frequency, salud AS health, diag_heart AS heart, altura AS height, imc, animo AS mind, non_pathological_background, situation, cintura AS waist, peso AS weight, examination_id, created_at, updated_at FROM nutrimind_tests WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    18 => 'SELECT id, activities, examination_id, created_at, updated_at FROM psychological_plans WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    19 => 'SELECT id, CASE WHEN bad_in_morning = "si" THEN TRUE WHEN bad_in_morning = "no" THEN FALSE ELSE NULL END AS bad_in_morning, CASE WHEN lost_concentration = "si" THEN TRUE WHEN lost_concentration = "no" THEN FALSE ELSE NULL END AS lost_concentration, CASE WHEN lost_confidence = "si" THEN TRUE WHEN lost_confidence = "no" THEN FALSE ELSE NULL END AS lost_confidence, CASE WHEN lost_hope = "si" THEN TRUE WHEN lost_hope = "no" THEN FALSE ELSE NULL END AS lost_hope, CASE WHEN lost_interest = "si" THEN TRUE WHEN lost_interest = "no" THEN FALSE ELSE NULL END AS lost_interest, CASE WHEN lost_weight = "si" THEN TRUE WHEN lost_weight = "no" THEN FALSE ELSE NULL END AS lost_weight, CASE WHEN low_energy = "si" THEN TRUE WHEN low_energy = "no" THEN FALSE ELSE NULL END AS low_energy, CASE WHEN result = "Ansiedad" THEN TRUE WHEN result = "Normal" THEN FALSE ELSE NULL END AS result, CASE WHEN slowed = "si" THEN TRUE WHEN slowed = "no" THEN FALSE ELSE NULL END AS slowed, time_spent, CASE WHEN wake_up_early = "si" THEN TRUE WHEN wake_up_early = "no" THEN FALSE ELSE NULL END AS wake_up_early, session_id, created_at, updated_at FROM anxiety_tests WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    20 => 'SELECT id, diastolic_value, CASE WHEN result = "Presión Sanguínea Óptima" THEN 1 WHEN result = "Presión Sanguínea Normal" THEN 2 WHEN result = "Presión Sanguínea Alta" THEN 3 WHEN result = "Presión Sanguínea Normal Alta" THEN 4 WHEN result = "Presión Baja (Rango Normal Para Atletas y Niños)" THEN 5 WHEN result = "Presión Sanguínea Baja" THEN 6 ELSE NULL END AS result, systolic_value, time_spent, session_id, created_at, updated_at FROM blood_pressures WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    21 => 'SELECT id, interpretation, time_spent, session_id, created_at, updated_at from cardiogramas WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    22 => 'SELECT id, level, recomendation, CASE WHEN result = "Normal" THEN FALSE WHEN result = "Elevado" THEN TRUE WHEN result = "<h3 style=\"color: green;\">Normal</h3>" THEN FALSE WHEN result = "<h3 style=\"color: red;\">Elevado</h3>" THEN TRUE ELSE NULL END AS result, time_spent, session_id, created_at, updated_at FROM cholesterol_levels WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    23 => 'SELECT id, feel_culpability, feel_infamous, feel_pointless, feel_sad, feel_unhappy, feel_useless, no_pleasure, really_am, CASE WHEN result = "Soportable" THEN 1 WHEN result = "Normal" THEN 2 WHEN result = "Depresion" THEN 3 WHEN result = "Depresion mayor" THEN 4 ELSE NULL END AS result, spoil, time_spent, session_id, created_at, updated_at FROM depression_tests WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    24 => 'SELECT id, CASE WHEN ayuna = 1 THEN TRUE WHEN ayuna = 0 THEN FALSE ELSE NULL END AS fast, CAST(level AS INT) AS level, recomendation, CASE WHEN result = "Nivel óptimo" THEN TRUE WHEN result = "Nivel no óptimo" THEN FALSE WHEN result = "<h4 style=\"color: green;\">Nivel óptimo</h4>" THEN TRUE WHEN result = "<h4 style=\"color: red;\">Nivel no óptimo</h4>" THEN FALSE ELSE NULL END AS result, time_spent, session_id, created_at, updated_at FROM glucose_levels WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    25 => 'SELECT id, gmdl, CAST(hct AS INT) AS hct, recomendation, CASE WHEN result = "Nivel Bajo" THEN 1 WHEN result = "Normal" THEN 2 WHEN result = "Elevado" THEN 3 ELSE NULL END AS result, time_spent, session_id, created_at, updated_at FROM hemoglobins WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    26 => 'SELECT id, fevone, fevoneratio, pkf, recomendation, CASE WHEN result = "Resultado moderado" THEN 1 WHEN result = "Nivel normal" THEN 2 WHEN result = "Resultado Severo" THEN 3 WHEN result = "<h3 style=\"color: green;\">Nivel normal</h3>" THEN 2 WHEN result = "<h3 style=\"color: red\">Resultado moderado</h3>" THEN 1 ELSE NULL END AS result, CAST(total_lm AS INT) AS total_lm, session_id, created_at, updated_at FROM spirometers WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    27 => 'SELECT id, CAST(level AS INT) level, recomendation, CASE WHEN result = "Elevado" THEN TRUE WHEN result = "<h3 id=\"exam_bad_result\">Elevado</h4>" THEN TRUE WHEN result = "Normal" THEN FALSE WHEN result = "<h3 style=\"color: green;\">Normal</h3>" THEN FALSE ELSE NULL END AS result, time_spent, session_id, created_at, updated_at FROM triglyceride_levels WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    28 => 'SELECT id, CASE WHEN hypertension = 1 THEN TRUE WHEN hypertension = 0 THEN FALSE ELSE NULL END AS hypertension, recomendation, CASE WHEN result = "Normal" THEN FALSE WHEN result = "Hay posibilidades de padecer un trastorno respiratorio durante el sueño" THEN TRUE ELSE NULL END AS result, CASE WHEN sleep_driving = 1 THEN TRUE WHEN sleep_driving = 0 THEN FALSE ELSE NULL END AS sleep_driving, CASE WHEN sleep_driving_frecuence = "Casi nunca o nunca" THEN 1 WHEN sleep_driving_frecuence = "1 a 2 veces por mes" THEN 2 WHEN sleep_driving_frecuence = "1 a 2 veces por semana" THEN 3 WHEN sleep_driving_frecuence = "3 a 4 veces por semana" THEN 4 WHEN sleep_driving_frecuence = "casi todos los dias" THEN 5 WHEN sleep_driving_frecuence = "no aplica" THEN 6 ELSE NULL END AS sleep_driving_frecuence, CASE WHEN snore = "false" THEN FALSE WHEN snore = "true" THEN TRUE WHEN snore = "no" THEN FALSE WHEN snore = "si" THEN TRUE WHEN snore = "no lo se" THEN FALSE ELSE NULL END AS snore, CASE WHEN snore_disturb = 1 THEN TRUE WHEN snore_disturb = 0 THEN FALSE ELSE NULL END AS snore_disturb, CASE WHEN snore_frecuence = "Casi nunca o nunca" THEN 1 WHEN snore_frecuence = "1 a 2 veces por mes" THEN 2 WHEN snore_frecuence = "1 a 2 veces por semana" THEN 3 WHEN snore_frecuence = "3 a 4 veces por semana" THEN 4 WHEN snore_frecuence = "Casi todos los dias" THEN 5 WHEN snore_frecuence = "no aplica" THEN 6 ELSE NULL END AS snore_frecuence, CASE WHEN snore_volume = "Respiración Fuerte" THEN 1 WHEN snore_volume = "tan alto como una conversacion" THEN 2 WHEN snore_volume = "mas alto que una conversacion" THEN 3 WHEN snore_volume = "muy alto, se puede escuchar desde habitaciones vecinas" THEN 4 WHEN snore_volume = "no aplica" THEN 5 ELSE NULL END AS snore_volume, CASE WHEN stop_breathing = "Casi nunca o nunca" THEN 1 WHEN stop_breathing = "1 a 2 veces por mes" THEN 2 WHEN stop_breathing = "1 a 2 veces por semana" THEN 3 WHEN stop_breathing = "3 a 4 veces por semana" THEN 4 WHEN stop_breathing = "Casi todos los dias" THEN 5 WHEN stop_breathing = "no aplica" THEN 6 ELSE NULL END AS stop_breathing, time_spent, CASE WHEN tired_day = "casi nunca o nunca" THEN 1 WHEN tired_day = "1 a 2 veces por mes" THEN 2 WHEN tired_day = "1 a 2 veces por semana" THEN 3 WHEN tired_day = "3 a 4 veces por semana" THEN 4 WHEN tired_day = "Casi todos los dias" THEN 5 WHEN tired_day = "no aplica" THEN 6 ELSE NULL END AS tired_day, CASE WHEN wake_up_tired = "casi nunca o nunca" THEN 1 WHEN wake_up_tired = "1 a 2 veces por mes" THEN 2 WHEN wake_up_tired = "1 a 2 veces por semana" THEN 3 WHEN wake_up_tired = "3 a 4 veces por semana" THEN 4 WHEN wake_up_tired = "Casi todos los dias" THEN 5 WHEN wake_up_tired = "no aplica" THEN 6 ELSE NULL END AS wake_up_tired, session_id, created_at, updated_at FROM dreams WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;'
  }[queryIndex]
end

def getDf(index)
  Daru::DataFrame.new(@myclient.query(getQuery(index)).to_a)
end

def copyToDB(table)
  @pgconn.exec("COPY #{table} FROM '/tmp/#{table}.csv' DELIMITER ',' CSV HEADER;")
end

def setSessionRisk
  @pgconn.exec("WITH session_risks AS (SELECT sessions.id AS sessionid, CASE WHEN emotional_states.risk = 3 THEN 3 WHEN health_states.risk = 3 THEN 3 WHEN waist_widths.risk = 3 THEN 3 ELSE body_mass_indices.risk END AS sessionrisk FROM sessions, body_mass_indices, emotional_states, health_states, waist_widths WHERE body_mass_indices.session_id = sessions.id AND emotional_states.session_id = sessions.id AND health_states.session_id = sessions.id AND waist_widths.session_id = sessions.id ) UPDATE sessions SET risk = session_risks.sessionrisk FROM session_risks WHERE sessions.id = session_risks.sessionid;")
    
  @pgconn.exec("UPDATE sessions SET risk = body_mass_indices.risk FROM body_mass_indices WHERE sessions.id = body_mass_indices.session_id AND sessions.risk = 0;")
end

def primaryKeySequence(table)
  @pgconn.exec("SELECT setval(pg_get_serial_sequence('#{table}', 'id'), MAX(id)) FROM #{table};")
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
    17 => 'nutrimind_tests',
    18 => 'psychological_plans',
    19 => 'anxieties',
    20 => 'blood_pressures',
    21 => 'electrocardiograms',
    22 => 'cholesterol_levels',
    23 => 'depressions',
    24 => 'glucose_levels',
    25 => 'hemoglobins',
    26 => 'spirometries',
    27 => 'triglyceride_levels',
    28 => 'dreams'
  }

  tables.each do |index, table|
    puts table
    begin
      df = getDf(index)
      df.write_csv("/tmp/#{table}.csv")
      #puts `cp evital_production/#{table}.csv /tmp/.`
      copyToDB(table)
      primaryKeySequence(table)
      File.delete("/tmp/#{table}.csv") if File.exist?("/tmp/#{table}.csv")
    rescue Exception => e
      puts e
      File.delete("/tmp/#{table}.csv") if File.exist?("/tmp/#{table}.csv")
      exit
    end
  end
  begin
    primaryKeySequence('users')
    setSessionRisk
  end
end
