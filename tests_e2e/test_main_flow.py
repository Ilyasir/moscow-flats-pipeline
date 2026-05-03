import random
import re
import string

from playwright.sync_api import Page, expect

BASE_URL = "http://localhost:5173"

def get_random_string(length=8):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))

def test_user_registration_and_login_flow(page: Page):
    username = f"user_{get_random_string(5)}"
    password = "testpassword123"
    page.goto(BASE_URL)
    page.locator(".login-btn").click()
    page.click("text=Еще нет аккаунта? Создать")
    page.fill('.auth-box input[placeholder="Логин"]', username)
    page.fill('.auth-box input[placeholder="Пароль"]', password)
    page.click('.auth-box button:has-text("Зарегистрироваться")')
    expect(page.locator(".auth-box")).not_to_be_visible()
    page.locator(".login-btn").click()
    if page.locator("text=Уже есть профиль? Войти").is_visible():
        page.click("text=Уже есть профиль? Войти")
    page.fill('.auth-box input[placeholder="Логин"]', username)
    page.fill('.auth-box input[placeholder="Пароль"]', password)
    page.locator(".auth-box button").click()
    expect(page.locator(".profile-chip")).to_be_visible(timeout=15000)
    expect(page.locator(".user-block")).to_contain_text(username)
    page.click(".profile-chip")
    page.click("text=Выйти из системы")
    expect(page.locator(".login-btn")).to_be_visible()

def test_filters_and_tags(page: Page):
    page.goto(BASE_URL)
    okrug_select = page.locator("select").first
    okrug_select.select_option("ЦАО")
    page.wait_for_selector(".loader-inline", state="hidden")
    first_tag = page.locator(".flat-tags span").first
    expect(first_tag).to_contain_text("ЦАО")

def test_flat_card_content(page: Page):
    page.goto(BASE_URL)
    card = page.locator(".flat-card").first
    expect(card).to_be_visible()
    expect(card).to_contain_text("20,000,000")

def test_simple_page_load(page: Page):
    page.goto(BASE_URL)
    expect(page).to_have_title(re.compile(r".+"))
    expect(page.locator(".flat-card").first).to_be_visible()