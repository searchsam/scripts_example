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
    0 => 'SELECT users.id, "505" AS area_code, users.auth_token, people.birth_date as birthday, users.current_sign_in_at, users.current_sign_in_ip, NULL AS device_id, users.email, users.encrypted_password, NULL AS firebase_id, users.last_sign_in_at, users.last_sign_in_ip, users.passwd_login, people.phone_number AS phone, NULL AS provider, users.remember_created_at, users.reset_password_sent_at, users.reset_password_token, CASE WHEN users.role = "admin" THEN 1 WHEN users.role = "general" THEN 2 WHEN users.role = "doctor" THEN 3 WHEN users.role = "employee" THEN 4 WHEN users.role = "evaluator" THEN 5 ELSE NULL END AS role, users.sign_in_count, users.created_at, users.updated_at FROM users, people WHERE people.user_id = users.id ORDER BY users.created_at ASC;',
    1 => 'SELECT people.id, NULL AS age, people.last_name, people.name, CASE WHEN people.is_male = 1 THEN TRUE WHEN people.is_male = 0 THEN FALSE ELSE NULL END AS sex, users.stripe_id, users.stripe_subscription_id, people.user_id, people.created_at, people.updated_at FROM people, users WHERE people.user_id = users.id AND people.user_id IS NOT NULL ORDER BY people.created_at ASC;',
    2 => 'SELECT id, available, NULL AS age, NULL AS last_name, last_seen_at, NULL AS name, NULL AS sex, user_id, created_at, updated_at FROM doctors WHERE user_id IS NOT NULL ORDER BY created_at ASC;',
    3 => 'SELECT sessions.id, sessions.person_id AS patient_id, sessions.user_id, sessions.created_at, sessions.updated_at FROM sessions WHERE sessions.person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL) ORDER BY sessions.created_at ASC;',
    4 => 'SELECT id, CASE WHEN examination_type = "chat" THEN 1 WHEN examination_type = "video" THEN 2 WHEN examination_type = "expediente" THEN 3 WHEN examination_type = "NUTRIMIND_TEST_PLAN" THEN 4 WHEN examination_type = "voice" THEN 5 WHEN examination_type = "Plan_Nutricional" THEN 6 ELSE NULL END AS examination_type, finished_at, finished, CASE WHEN service_type = "paid" THEN 1 WHEN service_type = "free" THEN 2 ELSE NULL END AS service, taken, twilio_channel_name, twilio_channel_sid, doctor_id AS coach_id, person_id AS patient_id, created_at, updated_at FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL) ORDER BY created_at ASC;',
    5 => 'SELECT id, appointment_date AS date, DATE(appointment_date) AS day, TIME(appointment_date) AS hour, reason, CASE WHEN status = "scheduled" THEN 1 WHEN status = "pending" THEN 2 WHEN status = "finished" THEN 3 ELSE NULL END AS status, taken, CASE WHEN ap_type = "plan" THEN 1 ELSE NULL END AS appointment_type, doctor_id AS coach_id, person_id AS patient_id, created_at, updated_at FROM appointments WHERE person_id IS NOT NULL AND doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL) ORDER BY created_at ASC;',
    6 => 'SELECT id, end_date, start_date, status, person_id AS patient_id, created_at, updated_at FROM memberships ORDER BY created_at ASC;',
    7 => 'SELECT id, height, imc, cintura AS waist, CASE WHEN cintura_cat = "Normal" THEN 1 WHEN cintura_cat = "Riesgo aumentado" THEN 2 WHEN cintura_cat = "Riesgo muy aumentado" THEN 3 ELSE NULL END AS waist_category, weight, CASE WHEN result = "Infrapeso" THEN 1 WHEN result = "Peso Normal" THEN 2 WHEN result = "Sobrepeso" THEN 3 WHEN result = "Obesidad" THEN 4 ELSE NULL END AS weight_category, session_id, created_at, updated_at FROM body_mass_indices WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    8 => 'SELECT id, comments AS comment, friday_breakfast, friday_dinner, friday_lunch, friday_snack_dinner, friday_snack_lunch, friday_snack, monday_breakfast, monday_dinner, monday_lunch, monday_snack_dinner, monday_snack_lunch, monday_snack, saturday_breakfast, saturday_dinner, saturday_lunch, saturday_snack_dinner, saturday_snack_lunch, saturday_snack, sunday_breakfast, sunday_dinner, sunday_lunch, sunday_snack_dinner, sunday_snack_lunch, sunday_snack, thursday_breakfast, thursday_dinner, thursday_lunch, thursday_snack_dinner, thursday_snack_lunch, thursday_snack, tuesday_breakfast, tuesday_dinner, tuesday_lunch, tuesday_snack_dinner, tuesday_snack_lunch, tuesday_snack, wednesday_breakfast, wednesday_dinner, wednesday_lunch, wednesday_snack_dinner, wednesday_snack_lunch, wednesday_snack, examination_id, created_at, updated_at FROM nutritional_plans WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    9 => 'SELECT id, NULL AS chat, plan, reason_attention AS reason, examination_id, created_at, updated_at FROM soaps WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    10 => 'SELECT id, activities, examination_id, created_at, updated_at FROM psychological_plans WHERE examination_id IN (SELECT id FROM examinations WHERE doctor_id IS NOT NULL AND doctor_id IN (SELECT id FROM doctors WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    11 => 'SELECT id, CASE WHEN bad_in_morning = "si" THEN TRUE WHEN bad_in_morning = "no" THEN FALSE ELSE NULL END AS bad_in_morning, CASE WHEN lost_concentration = "si" THEN TRUE WHEN lost_concentration = "no" THEN FALSE ELSE NULL END AS lost_concentration, CASE WHEN lost_confidence = "si" THEN TRUE WHEN lost_confidence = "no" THEN FALSE ELSE NULL END AS lost_confidence, CASE WHEN lost_hope = "si" THEN TRUE WHEN lost_hope = "no" THEN FALSE ELSE NULL END AS lost_hope, CASE WHEN lost_interest = "si" THEN TRUE WHEN lost_interest = "no" THEN FALSE ELSE NULL END AS lost_interest, CASE WHEN lost_weight = "si" THEN TRUE WHEN lost_weight = "no" THEN FALSE ELSE NULL END AS lost_weight, CASE WHEN low_energy = "si" THEN TRUE WHEN low_energy = "no" THEN FALSE ELSE NULL END AS low_energy, CASE WHEN result = "Ansiedad" THEN TRUE WHEN result = "Normal" THEN FALSE ELSE NULL END AS result, CASE WHEN slowed = "si" THEN TRUE WHEN slowed = "no" THEN FALSE ELSE NULL END AS slowed, time_spent, CASE WHEN wake_up_early = "si" THEN TRUE WHEN wake_up_early = "no" THEN FALSE ELSE NULL END AS wake_up_early, session_id, created_at, updated_at FROM anxiety_tests WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    12 => 'SELECT id, diastolic_value AS diastolic, CASE WHEN result = "Presión Sanguínea Óptima" THEN 1 WHEN result = "Presión Sanguínea Normal" THEN 2 WHEN result = "Presión Baja (Rango Normal Para Atletas y Niños)" THEN 3 WHEN result = "Presión Sanguínea Normal Alta" THEN 4 WHEN result = "Presión Sanguínea Baja" THEN 5 WHEN result = "Presión Sanguínea Alta" THEN 6 ELSE NULL END AS result, systolic_value AS systolic, time_spent, session_id, created_at, updated_at FROM blood_pressures WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    13 => 'SELECT id, interpretation, time_spent, session_id, created_at, updated_at from cardiogramas WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROMsessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    14 => 'SELECT id, level, recomendation, CASE WHEN result = "Normal" THEN 1 WHEN result = "Elevado" THEN 2 WHEN result = "<h3 style=\"color: green;\">Normal</h3>" THEN 1 WHEN result = "<h3 style=\"color: red;\">Elevado</h3>" THEN 2 ELSE NULL END AS result, time_spent, session_id, created_at, updated_at FROM cholesterol_levels WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    15 => 'SELECT id, feel_culpability, feel_infamous, feel_pointless, feel_sad, feel_unhappy, feel_useless, no_pleasure, really_am, CASE WHEN result = "Normal" THEN 1 WHEN result = "Soportable" THEN 2 WHEN result = "Depresion" THEN 3 WHEN result = "Depresion mayor" THEN 4 ELSE NULL END AS result, spoil, time_spent, session_id, created_at, updated_at FROM depression_tests WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    16 => 'SELECT id, CASE WHEN ayuna = 1 THEN TRUE WHEN ayuna = 0 THEN FALSE ELSE NULL END AS fast, CAST(level AS UNSIGNED) AS level, recomendation, CASE WHEN result = "Nivel óptimo" THEN TRUE WHEN result = "Nivel no óptimo" THEN FALSE WHEN result = "<h4 style=\"color: green;\">Nivel óptimo</h4>" THEN FALSE WHEN result = "<h4 style=\"color: red;\">Nivel no óptimo</h4>" THEN TRUE ELSE NULL END AS result, time_spent, session_id, created_at, updated_at FROM glucose_levels WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    17 => 'SELECT id, gmdl, CAST(hct AS UNSIGNED) AS hct, recomendation, CASE WHEN result = "Normal" THEN 1 WHEN result = "Nivel Bajo" THEN 2 WHEN result = "Elevado" THEN 3 ELSE NULL END AS result, time_spent, session_id, created_at, updated_at FROM hemoglobins WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    18 => 'SELECT id, fevone, fevoneratio, pkf, recomendation, CASE WHEN result = "Nivel normal" THEN 1 WHEN result = "Resultado moderado" THEN 2 WHEN result = "Resultado Severo" THEN 3 WHEN result = "<h3 style=\"color: green;\">Nivel normal</h3>" THEN 1 WHEN result = "<h3 style=\"color: red\">Resultado moderado</h3>" THEN 2 ELSE NULL END AS result, CAST(total_lm AS UNSIGNED) AS total_lm, session_id, created_at, updated_at FROM spirometers WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    19 => 'SELECT id, CAST(level AS UNSIGNED) level, recomendation, CASE WHEN result = "Elevado" THEN 2 WHEN result = "<h3 id=\"exam_bad_result\">Elevado</h4>" THEN 2 WHEN result = "Normal" THEN 1 WHEN result = "<h3 style=\"color: green;\">Normal</h3>" THEN 1 ELSE NULL END AS result, time_spent, session_id, created_at, updated_at FROM triglyceride_levels WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;',
    20 => 'SELECT id, CASE WHEN hypertension = 1 THEN TRUE WHEN hypertension = 0 THEN FALSE ELSE NULL END AS hypertension, recomendation, CASE WHEN result = "Normal" THEN FALSE WHEN result = "Hay posibilidades de padecer un trastorno respiratorio durante el sueño" THEN TRUE ELSE NULL END AS result, CASE WHEN sleep_driving = 1 THEN TRUE WHEN sleep_driving = 0 THEN FALSE ELSE NULL END AS sleep_driving, CASE WHEN sleep_driving_frecuence = "Casi nunca o nunca" THEN 0 WHEN sleep_driving_frecuence = "1 a 2 veces por semana" THEN 1 WHEN sleep_driving_frecuence = "3 a 4 veces por semana" THEN 2 WHEN sleep_driving_frecuence = "1 a 2 veces por mes" THEN 3 WHEN sleep_driving_frecuence = "casi todos los dias" THEN 4 WHEN sleep_driving_frecuence = "no aplica" THEN NULL ELSE NULL END AS sleep_driving_frecuence, CASE WHEN snore = "false" THEN 1 WHEN snore = "true" THEN 0 WHEN snore = "no" THEN 1 WHEN snore = "si" THEN 0 WHEN snore = "no lo se" THEN 2 ELSE NULL END AS snore, CASE WHEN snore_disturb = 1 THEN FALSE WHEN snore_disturb = 0 THEN TRUE ELSE NULL END AS snore_disturb, CASE WHEN snore_frecuence = "Casi nunca o nunca" THEN 0 WHEN snore_frecuence = "1 a 2 veces por semana" THEN 1 WHEN snore_frecuence = "3 a 4 veces por semana" THEN 2 WHEN snore_frecuence = "1 a 2 veces por mes" THEN 3 WHEN snore_frecuence = "Casi todos los dias" THEN 4 WHEN snore_frecuence = "no aplica" THEN NULL ELSE NULL END AS snore_frecuence, CASE WHEN snore_volume = "Respiración Fuerte" THEN 0 WHEN snore_volume = "tan alto como una conversacion" THEN 1 WHEN snore_volume = "mas alto que una conversacion" THEN 2 WHEN snore_volume = "muy alto, se puede escuchar desde habitaciones vecinas" THEN 3 WHEN snore_volume = "no aplica" THEN NULL ELSE NULL END AS snore_volume, CASE WHEN stop_breathing = "Casi nunca o nunca" THEN 0 WHEN stop_breathing = "1 a 2 veces por semana" THEN 1 WHEN stop_breathing = "3 a 4 veces por semana" THEN 2 WHEN stop_breathing = "1 a 2 veces por mes" THEN 3 WHEN stop_breathing = "Casi todos los dias" THEN 4 WHEN stop_breathing = "no aplica" THEN NULL ELSE NULL END AS stop_breathing, time_spent, CASE WHEN tired_day = "casi nunca o nunca" THEN 0 WHEN tired_day = "1 a 2 veces por semana" THEN 1 WHEN tired_day = "3 a 4 veces por semana" THEN 2 WHEN tired_day = "1 a 2 veces por mes" THEN 3 WHEN tired_day = "Casi todos los dias" THEN 4 WHEN tired_day = "no aplica" THEN NULL ELSE NULL END AS tired_day, CASE WHEN wake_up_tired = "casi nunca o nunca" THEN 0 WHEN wake_up_tired = "1 a 2 veces por semana" THEN 1 WHEN wake_up_tired = "3 a 4 veces por semana" THEN 2 WHEN wake_up_tired = "1 a 2 veces por mes" THEN 3 WHEN wake_up_tired = "Casi todos los dias" THEN 4 WHEN wake_up_tired = "no aplica" THEN NULL ELSE NULL END AS wake_up_tired, session_id, created_at, updated_at FROM dreams WHERE session_id IS NOT NULL AND session_id IN (SELECT id FROM sessions WHERE person_id IN (SELECT id FROM people WHERE user_id IS NOT NULL)) ORDER BY created_at ASC;'
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
    0 => 'users',
    1 => 'patients',
    2 => 'coaches',
    3 => 'sessions',
    4 => 'examinations',
    5 => 'appointments',
    6 => 'memberships',
    7 => 'body_mass_indices',
    8 => 'nutritional_plans',
    9 => 'soaps',
    10 => 'psychological_plans',
    11 => 'anxieties',
    12 => 'blood_pressures',
    13 => 'electrocardiograms',
    14 => 'cholesterol_levels',
    15 => 'depressions',
    16 => 'glucose_levels',
    17 => 'hemoglobins',
    18 => 'spirometries',
    19 => 'triglyceride_levels',
    20 => 'dreams'
  }

  tables.each do |index, table|
    puts table
    begin
      df = getDf(index)
      df.write_csv("/tmp/#{table}.csv")
      # puts `cp evital_production/#{table}.csv /tmp/.`
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
