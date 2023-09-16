import pandas as pd
import yaml
from sshtunnel import SSHTunnelForwarder
import pymysql
import altair as alt
import streamlit as st

st.set_page_config(
    page_title="MarzbanStat",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="collapsed"
)


@st.cache_data(ttl=300, show_spinner="Загрузка данных") 
def getdata(ssh_host,ssh_port,ssh_user,ssh_pass,sql_hostname,sql_port,sql_username,sql_password,sql_main_database, query):
    with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_password=ssh_pass,
        remote_bind_address=(sql_hostname, sql_port)) as tunnel:
        conn = pymysql.connect(host=sql_hostname, user=sql_username,
            passwd=sql_password, db=sql_main_database,
            port=tunnel.local_bind_port)
        df = pd.read_sql_query(query, conn)
        conn.close()
    return df


def data_from_marzban(query):
    with open('config.yaml') as file:
        config = yaml.safe_load(file)
    df = getdata(config['credentials']['ssh_host'],config['credentials']['ssh_port'],config['credentials']['ssh_user'],
                            config['credentials']['ssh_pass'],config['credentials']['sql_hostname'],config['credentials']['sql_port'],
                            config['credentials']['sql_username'],config['credentials']['sql_password'],config['credentials']['sql_main_database'], query)
    df["used_traffic_gb"] = df["used_traffic"] / 1073741824
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"])
        df["hour"] = df["created_at"].dt.hour
    return df 

def last_hour_users(df):
    df["created_at"] = pd.to_datetime(df["created_at"])
    max_date = df["created_at"].max()
    last_hour_users = df[df["created_at"] == max_date]
    return last_hour_users

def users_by_hours(df):
    hourly_counts = df.groupby(df["created_at"].dt.hour)["username"].nunique()
    hourly_counts = hourly_counts.reset_index()
    hourly_counts = hourly_counts.rename(columns={"username": "Connections"})
    return hourly_counts

def traffic_by_hours(df):
    hourly_counts = df.groupby("hour")["used_traffic_gb"].sum().reset_index()
    hourly_counts["used_traffic_gb"] = hourly_counts["used_traffic_gb"].round(1)
    hourly_counts = hourly_counts.rename(columns={"used_traffic_gb": "traffic"})
    return hourly_counts

def traffic_by_users(df):
    user_traffic_data = df.groupby("username")["used_traffic_gb"].agg(
        total_traffic_gb = 'sum',
        connections = 'count'
    )
    user_traffic_data = user_traffic_data.reset_index()
    user_traffic_data = user_traffic_data.sort_values(by=['total_traffic_gb', 'connections'], ascending=[False, False])
    return user_traffic_data



df = data_from_marzban("""
                        select (
                                `a`.`created_at` + interval 3 hour
                            ) AS `created_at`,
                            `a`.`used_traffic` AS `used_traffic`,
                            ifnull(`n`.`name`, 'Main') AS `node`,
                            `u`.`username` AS `username`
                        from ( (
                                    `node_user_usages` `a`
                                    left join `users` `u` on( (`u`.`id` = `a`.`user_id`))
                                )
                                left join `nodes` `n` on( (`n`.`id` = `a`.`node_id`))
                            )
                        where (
                                `a`.`created_at` >= concat( (curdate() - interval 1 day),
                                    ' 21:00:00'
                                )
                            )
                        order by `a`.`created_at` desc
                       """)
df_last_hour_users = last_hour_users(df)
df_users_by_hours = users_by_hours(df)
stat_by_users_today = traffic_by_users(df)
stat_by_users_last_hour = traffic_by_users(df_last_hour_users)
traffic_by_users_last_hour = traffic_by_users(df_last_hour_users)
traffic_by_hours_today = traffic_by_hours(df)
df_all_dates = data_from_marzban("""select `users_usage`.`username` AS `username`,
        count(`users_usage`.`created_at`) AS `cnt_connections`,
        sum(`users_usage`.`used_traffic`) AS `used_traffic`,
        min(`users_usage`.`created_at`) AS `first_conn`,
        max(`users_usage`.`created_at`) AS `last_conn`, (
            to_days(
                max(`users_usage`.`created_at`)
            ) - to_days(
                min(`users_usage`.`created_at`)
            )
        ) AS `lifetime_days`
    from `users_usage`
    group by
        `users_usage`.`username`
    order by
        count(`users_usage`.`created_at`) desc""")





st.header("Сегодня по часам") 
col1, col2 = st.columns(2)
with col1:
    bars = alt.Chart(df_users_by_hours).mark_bar().encode(
        x=alt.X('created_at:N', axis=alt.Axis(title='Час')),
        y=alt.Y('sum(Connections):Q', stack='zero', axis=alt.Axis(title='Подключений')),
        color=alt.Color('Connections', title='Кол-во')
    )
    
    text = alt.Chart(df_users_by_hours).mark_text(dx=0, dy=-10, align='center', color='white').encode(
        x=alt.X('created_at:N', axis=alt.Axis(title='Час')),
        y=alt.Y('sum(Connections):Q', stack='zero', axis=alt.Axis(title='Подключений')),
        text=alt.Text('sum(Connections):Q')
    )
    
    
    mean_line = alt.Chart(df_users_by_hours).transform_aggregate(
        mean_connections='mean(Connections)'
    ).mark_rule(color='lightblue', strokeDash=[10, 5], opacity=0.5).encode(
        y='mean(mean_connections):Q'
    )
    
    st.altair_chart(bars+text+mean_line, use_container_width=True)
with col2:
    #-----------------------траффик
    bars = alt.Chart(traffic_by_hours_today).mark_bar().encode(
        x=alt.X('hour:N', axis=alt.Axis(title='Час')),
        y=alt.Y('sum(traffic):Q', stack='zero', axis=alt.Axis(title='GB')),
        color=alt.Color('traffic', title='GB')
    )

    text = alt.Chart(traffic_by_hours_today).mark_text(dx=0, dy=-10, align='center', color='white').encode(
        x=alt.X('hour:N', axis=alt.Axis(title='Час')),
        y=alt.Y('sum(traffic):Q', stack='zero', axis=alt.Axis(title='GB')),
        text=alt.Text('sum(traffic):Q')
    )

    
    mean_line = alt.Chart(traffic_by_hours_today).transform_aggregate(
        mean_traffic='mean(traffic)'
    ).mark_rule(color='lightblue', strokeDash=[10, 5], opacity=0.5).encode(
        y='mean(mean_traffic):Q'
    )

    st.altair_chart(bars+text+mean_line, use_container_width=True)


# Переименование колонок
user_traffic_data = stat_by_users_today.rename(columns={"username": "Имя пользователя", "total_traffic_gb": "Трафик (ГБ)", "connections": "Подключения"})
stat_by_users_last_hour = stat_by_users_last_hour.rename(columns={"username": "Имя пользователя", "total_traffic_gb": "Трафик (ГБ)"})

# Получение топ 5 пользователей по подключениям и трафику
top5_connections = user_traffic_data.nlargest(5, 'Подключения')[['Имя пользователя', 'Подключения']].reset_index(drop=True)
top5_traffic = user_traffic_data.nlargest(5, 'Трафик (ГБ)')[['Имя пользователя', 'Трафик (ГБ)']].reset_index(drop=True)
# Получение топ 5 пользователей по трафику за последний час
top5_last_hour_traffic = stat_by_users_last_hour.nlargest(5, 'Трафик (ГБ)')[['Имя пользователя', 'Трафик (ГБ)']].reset_index(drop=True)

st.subheader("Топ 5 пользователей")

col1, col2, col3 = st.columns(3)


with col1:
    st.write("По подключениям за день")
    st.dataframe(top5_connections, use_container_width=True)


with col2:
    st.write("По траффику за день")
    st.dataframe(top5_traffic, use_container_width=True)

with col3:
    st.write("По трафику за последний час")
    st.dataframe(top5_last_hour_traffic, use_container_width=True)




st.header("Общая статистика") 
# Переименование колонок
df_all_dates = df_all_dates.rename(columns={
    "username": "Имя пользователя",
    "cnt_connections": "Количество подключений",
    "lifetime_days": "Время жизни (дни)",
    "used_traffic_gb": "Трафик (ГБ)"
})


# Генерация топов и антитопов
top_traffic = df_all_dates.nlargest(5, 'Трафик (ГБ)')[['Имя пользователя', 'Трафик (ГБ)']]
top_connections = df_all_dates.nlargest(5, 'Количество подключений')[['Имя пользователя', 'Количество подключений']]
top_lifetime = df_all_dates.nlargest(5, 'Время жизни (дни)')[['Имя пользователя', 'Время жизни (дни)']]
anti_top_traffic = df_all_dates.nsmallest(5, 'Трафик (ГБ)')[['Имя пользователя', 'Трафик (ГБ)']]
anti_top_connections = df_all_dates.nsmallest(5, 'Количество подключений')[['Имя пользователя', 'Количество подключений']]


# Функция для создания гистограммы
def create_bar_chart(data, x, y, title):
    chart = alt.Chart(data).mark_bar().encode(
        x=alt.X(x, title=x),
        y=alt.Y(y, title=y)
    ).properties(
        title=title
    )
    return chart

# Гистограммы для топ 5 пользователей
col3, col4, col5 = st.columns([1, 1, 1])

with col3:
    st.altair_chart(create_bar_chart(top_traffic, 'Имя пользователя', 'Трафик (ГБ)', 'Топ 5 по траффику'), use_container_width=True)

with col4:
    st.altair_chart(create_bar_chart(top_connections, 'Имя пользователя', 'Количество подключений', 'Топ 5 по подключениям'), use_container_width=True)

with col5:
    st.altair_chart(create_bar_chart(top_lifetime, 'Имя пользователя', 'Время жизни (дни)', 'Топ 5 по времени жизни'), use_container_width=True)
# Колонки для топов и антитопов
col1, col2 = st.columns(2)

# Топ 5 пользователей
with col1:
    st.subheader("Топ 5 Пользователей")
    st.write("По траффику")
    st.dataframe(top_traffic, use_container_width=True)
    st.write("По подключениям")
    st.dataframe(top_connections, use_container_width=True)
    st.write("По времени жизни")
    st.dataframe(top_lifetime, use_container_width=True)

# Антитоп 5 пользователей
with col2:
    st.subheader("Антитоп 5 Пользователей")
    st.write("По траффику")
    st.dataframe(anti_top_traffic, use_container_width=True)
    st.write("По подключениям")
    st.dataframe(anti_top_connections, use_container_width=True)




with st.expander("Исходные данные", expanded=False):
    st.dataframe(df, use_container_width=True)
    st.dataframe(df_last_hour_users, use_container_width=True)
    st.dataframe(df_users_by_hours, use_container_width=True)
    st.dataframe(stat_by_users_today, use_container_width=True)
    st.dataframe(traffic_by_users_last_hour, use_container_width=True)
    st.dataframe(df_all_dates, use_container_width=True)

