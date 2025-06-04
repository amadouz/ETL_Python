def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df = pd.DataFrame(columns=table_attribs)
    r = requests.get(URL)
    soup = BeautifulSoup(r.content, 'html5lib')
    rows = soup.find_all('table')[0].find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) != 0:
           data_dict = {
            table_attribs[0] : cells[1].text.strip(),
            table_attribs[1] : float(cells[2].text.strip())
           }
           df = pd.concat([df,pd.DataFrame(data_dict, index=[0])], ignore_index = True)

    return df
