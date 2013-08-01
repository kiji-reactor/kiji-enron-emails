# Data collected from: https://www.cs.cmu.edu/~enron/
# Schema derived from http://www.isi.edu/~adibi/Enron/Enron_Dataset_Report.pdf

# Setup Environment
export ENRON_EMAIL_HOME=/home/lsheng/project/kijienronemail
export KIJI=kiji://.env/enron_email
export LIBS_DIR=${ENRON_EMAIL_HOME}/lib
export KIJI_CLASSPATH="${LIBS_DIR}/*"

# Setup tables
kiji install --kiji=${KIJI}
kiji-schema-shell --kiji=${KIJI} --file=${ENRON_EMAIL_HOME}/email_schema.ddl

