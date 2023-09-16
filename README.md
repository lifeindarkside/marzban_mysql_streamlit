# Marzban Dashboard Project

Этот дашборд создан для визуализации статистик в проекте [Marzban](https://github.com/Gozargah/Marzban), используя MySQL для хранения данных.

## Установка

### Шаг 1: Подготовка конфигурационного файла

Первым делом, вам нужно подготовить файл конфигурации. Скопируйте `config.yaml.example` в новый файл с именем `config.yaml` и заполните все необходимые поля соответствующими значениями вашей установки Marzban и базы данных MySQL.

Пример:

```yaml
credentials:
  ssh_host: 'your_ssh_host_here'
  ssh_port: your_ssh_port_here
  ssh_user: 'your_ssh_username_here'
  ssh_pass: 'your_ssh_password_here'
  sql_hostname: 'your_sql_hostname_here'
  sql_port: your_sql_port_here
  sql_username: 'your_sql_username_here'
  sql_password: 'your_sql_password_here'
  sql_main_database: 'your_sql_main_database_here'
```
### Шаг 2: Установка зависимостей

Перед запуском проекта убедитесь, что у вас установлен Python версии 3.8 или выше. Затем установите все необходимые зависимости, используя следующую команду в корневой директории проекта:

```sh
pip install -r requirements.txt
```
### Шаг 3: Запуск проекта

После того как вы установили все необходимые зависимости, вы можете запустить проект с помощью следующей команды в корневой директории проекта:

```sh
streamlit run main.py
```

