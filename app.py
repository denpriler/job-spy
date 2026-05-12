from fastapi import FastAPI
from jobspy import scrape_jobs

app = FastAPI()

@app.get("/jobs")
def jobs():
    # Настройки поиска под профиль Senior PHP/Laravel Full-Stack Engineer
    # с опытом в high‑load, iGaming/betting, Kafka, ClickHouse, Vue/Nuxt
    #
    # Целевые страны из отчёта: Кипр, Мальта, Нидерланды, Испания, Португалия
    # Чтобы охватить их все, указываем регион "European Union" — так поиск
    # не ограничится одной страной, а соберёт вакансии из этих юрисдикций.
    # Альтернативно можно запускать скрипт для каждой страны по очереди,
    # но текущий подход даёт широту.
    #
    # Поисковый термин скомбинирован:
    #   - ключевые технологии: Laravel, PHP, Symfony, full-stack
    #   - домены: iGaming, betting, fintech
    #   - признак релокации/визы (важно для не-ЕС кандидата с ВНЖ в Германии)
    search_term = (
        '(Laravel OR PHP OR Symfony) '
        '(full-stack OR backend) '
        '(iGaming OR betting OR fintech) '
        '(relocation OR "visa sponsorship")'
    )
    
    # Платформы: LinkedIn и Indeed дают максимум вакансий с релокацией,
    # Glassdoor тоже полезен, но часто требует сессию. Для стабильности
    # оставляем LinkedIn + Indeed.
    # JobSpy также поддерживает Google Jobs, ZipRecruiter – их можно добавить.
    sites = ["linkedin", "indeed"]
    
    # Количество результатов: увеличиваем до 50, т.к. релевантных вакансий
    # на рынке не очень много (по отчёту ~7–9 на Кипре/Мальте в моменте),
    # но больше образцов помогут провести анализ.
    results_limit = 50
    
    df = scrape_jobs(
        site_name=sites,
        search_term=search_term,
        location="European Union",   # Не привязано к одной стране
        results_wanted=results_limit,
        # Возможные дополнительные параметры (закомментированы, т.к. не во всех версиях jobspy есть):
        # country_indeed='eu',        # для европейского Indeed
        # remote_only=False,          # не только удалёнка (ищем с релокацией)
        # linkedin_fetch_description=True  # дотянуть полные описания
    )
    
    return df.to_dict("records")