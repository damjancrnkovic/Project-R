import requests
from bs4 import BeautifulSoup
import pandas as pd


news_URL = "https://zse.hr/hr/indeks/365?isin=HRZB00ICBEX6&tab=stock_news"
page = requests.get(news_URL)
home_page="zse.hr"
soup = BeautifulSoup(page.content, "html.parser")

#soup.prettify()



def link_output():
    news_URL = "https://zse.hr/hr/indeks/365?isin=HRZB00ICBEX6&tab=stock_news"
    page = requests.get(news_URL)
    home_page="zse.hr"
    soup = BeautifulSoup(page.content, "html.parser")
    
    dated_list = soup.find_all("div", class_="dated-list")
    news_cards = soup.find_all("ul", class_="annoucement-list")
    f = open('file.txt', 'w')
    f.truncate(0)
    for card in news_cards:
        link_li = card.find("li", class_="link-with-bullet")
        #print(card.find("li",))
        links = card.find_all("a")
        for link in links:
            full_link=home_page+link.get('href')
            if("zse.hr/hr" in full_link and "revizija" in link.text.lower()): 
                #print(link.text)
                print(full_link, end="\n",file=f)
            
        #print(card.text)
    
link_output()
link_file = open('links.txt','r')
links = link_file.read().split(sep = "\n")

data = pd.DataFrame(columns=['date', 'vrsta', 'added','removed'])
for link in links:
    page = requests.get(news_URL)
    soup = BeautifulSoup(page.content, "html.parser")
    title=soup.find("div",class_="page-title")
    #print(title.text)


