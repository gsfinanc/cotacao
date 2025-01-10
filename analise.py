import streamlit as st
import yfinance as yf
import requests
import time as time
import pandas as pd
import datetime as datetime
import streamlit_shadcn_ui as ui
import altair as alt

import requests
import zipfile
from io import BytesIO


st.set_page_config(
    page_title="GS FINANÇAS",
    page_icon="💲",
    layout='wide',
    initial_sidebar_state='expanded',
    menu_items={
        'Report a Bug': "mailto:gsfinancas.oficial@gmail.com",
        'About': 'Aplicativo desenvolvido por Edson Barboza com objetivo de realizar acompanhamento de ações.'
    })

@st.cache_data(ttl=3600)
def get_acoes():
    # Definindo os símbolos das criptomoedas
    symbols = [
               # Ações
               '^BVSP', 'CXSE3.SA', 'PETR4.SA', 'DIRR3.SA', 'EQTL3.SA', 'SANB11.SA','ITUB4.SA', \
               'ALUP11.SA', 'BBAS3.SA', 'CPLE6.SA', 'CYRE3.SA', 'VIVA3.SA', 'PRIO3.SA',\
                'WEGE3.SA', 'VALE3.SA', 'GMAT3.SA', 'IGTI11.SA', 'SUZB3.SA',\
                # Cripto de baixo risco
                'BTC-USD', 'ETH-USD'\
                # Cripto de médio risco
                'LINK-USD', 'TON-USD', 'ATOM-USD', 'SOL-USD',  'AVAX-USD', 'ARB-USD', 'OP-USD',\
                # Cripto de alto risco    
                'APT-USD', 'SUI20947-USD', 'LDO-USD', 'ME-USD'\
                # ETF exterior
                'VOO', 'QQQ', 'ACWI', 'HACK', 'VUG', 'VB']

    # Função para coletar e processar os dados
    def get_crypto_data(symbol):
        # Baixando os dados históricos (preço) e os dividendos
        data = yf.download(symbol, period='1y')
        dividends = yf.Ticker(symbol).dividends  # Coletando os dividendos
        
        # Resetando o índice para transformar o índice em uma coluna
        data = data.reset_index()
        data['Symbol'] = symbol  # Adicionar a coluna com o símbolo
        
        # Adicionar a coluna de dividendos ao DataFrame
        data['Dividends'] = data['Date'].map(lambda x: dividends.get(x, None))
        
        return data

    # Coletando e concatenando os dados
    dfs = [get_crypto_data(symbol) for symbol in symbols]
    df = pd.concat(dfs, ignore_index=True)

    df['Variação'] = df['Close'] - df['Open']

    # Definir a zona de data (timezone) para a coluna 'Date' (exemplo: 'America/Sao_Paulo')
    df['Date'] = df['Date'].dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')

    return df

@st.cache_data(ttl=43200)
def get_fundos():
    ano = "2024"
    # Criar uma lista para armazenar os DataFrames de cada mês
    dados_completos = []
    # Loop para iterar sobre todos os meses do ano

    for mes in range(1, 13):
        # Formatar o mês com dois dígitos (ex: '01', '02', ...)
        mes_formatado = f"{mes:02d}"
        # Criar a URL para o mês correspondente
        url = f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{ano}{mes_formatado}.zip'

        print(f"Baixando dados do mês: {mes_formatado}/{ano}")

        # Fazer o download do arquivo ZIP
        # download = requests.get(url, verify="/caminho/para/certificado.pem") esse código passou a dar erro
        download = requests.get(url, verify=True)
        # Verificar se o download foi bem-sucedido
        if download.status_code == 200:
            # Abrir o arquivo ZIP a partir do conteúdo baixado
            arquivo_zip = zipfile.ZipFile(BytesIO(download.content))
            
            # Ler o arquivo CSV dentro do ZIP
            dados_fundos = pd.read_csv(arquivo_zip.open(arquivo_zip.namelist()[0]), sep=";", encoding='ISO-8859-1', low_memory=False)

            # Renomear coluna se necessário pois a partir de outubro/24 a coluna CNPJ_FUNDO foi alterada
            if 'CNPJ_FUNDO_CLASSE' in dados_fundos.columns:
                dados_fundos.rename(columns={'CNPJ_FUNDO_CLASSE': 'CNPJ_FUNDO'}, inplace=True)

            # Filtrar os dados com base no CNPJ após garantir o nome correto da coluna
            dados_fundos = dados_fundos[dados_fundos['CNPJ_FUNDO'].str.contains(
                "20.147.389/0001-00|34.172.497/0001-47|47.612.737/0001-29|36.249.317/0001-03", 
                na=False
            )]
            # Adicionar os dados do mês ao DataFrame completo
            dados_completos.append(dados_fundos)
        else:
            print(f"Erro ao baixar dados para {mes_formatado}/{ano}")
        
    # Concatenar todos os DataFrames em um único DataFrame
    df_fundos = pd.concat(dados_completos, ignore_index=True)
    dados_fundos = dados_fundos.drop(['RESG_DIA', 'CAPTC_DIA'], axis=1)
    return df_fundos

@st.cache_data(ttl=86400)
def get_name_fundos():
    df_name_fundos = pd.read_csv('https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv',
                             sep = ";", encoding = 'ISO-8859-1', low_memory=False)
    df_name_fundos = df_name_fundos[['CNPJ_FUNDO', 'DENOM_SOCIAL']]
    df_name_fundos = df_name_fundos.drop_duplicates()
    return df_name_fundos

@st.cache_data(ttl=86400)
def get_cdi():
    # site para consultar o codigo para tipo de consulta
    # https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries
    #taxa selic 12, cdi 4398

    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=01/01/2024&dataFinal=31/12/2024"
    response = requests.get(url)
    dados = response.json()
    # Converter para DataFrame
    df_cdi = pd.DataFrame(dados)
    # Converter a coluna 'data' para o tipo datetime
    df_cdi['data'] = pd.to_datetime(df_cdi['data'], format='%d/%m/%Y')
    # Converter a coluna 'valor' para o tipo float
    df_cdi['valor'] = df_cdi['valor'].astype(float)
    # Calculando a variação percentual dia a dia
    df_cdi = df_cdi.rename(columns={'valor': 'Close', 'data': 'Date'})
    df_cdi['Symbol'] = df_cdi['Symbol'] = 'CDI'
    return df_cdi

def get_dolar():
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados?formato=json&dataInicial=01/01/2024&dataFinal=31/12/2024"
    response = requests.get(url)
    dados = response.json()
    # Converter para DataFrame
    df_dolar = pd.DataFrame(dados)
    # Converter a coluna 'data' para o tipo datetime
    df_dolar['data'] = pd.to_datetime(df_dolar['data'], format='%d/%m/%Y')
    # Converter a coluna 'valor' para o tipo float
    df_dolar['valor'] = df_dolar['valor'].astype(float)
    # Calculando a variação percentual dia a dia
    df_dolar = df_dolar.rename(columns={'valor': 'Close', 'data': 'Date'})
    df_dolar['Symbol'] = df_dolar['Symbol'] = 'Dolar'
    return df_dolar

class Application:
    def __init__(self):
        self.df = get_acoes()
        self.display_data()
        self.card()
        self.navegacao()
    
    def navegacao(self):
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(['Análise diária', 'Crescimento', 'Variação %', 'Volume', 
                                                      'Dividendo', 'Hora de vender', 'Notícias'])     
        @staticmethod
        def get_noticias(query):
            api_key = "4902e399258141fcbcd281a0d559b41a"  # Substitua pela sua chave da News API
            # api_key = "68b226a1179b48a7ac5bb607b0ff0af0"  # Substitua pela sua chave da News API
            url = f"https://newsapi.org/v2/everything?q={query}&sortBy=publishedAt&language=pt&apiKey={api_key}"
            try:
                response = requests.get(url)
                response.raise_for_status()  # Lança um erro se o status não for 200
                data = response.json()
                if "articles" in data and data["articles"]:  # Verifica se há artigos
                    return data["articles"][:6]  # Retorna os três primeiros artigos
                else:
                    return None
            except requests.exceptions.RequestException as e:
                st.error(f"Erro ao acessar a API de notícias: {e}")
                return None

        def show_news(articles):
            st.markdown('')
            # st.markdown('---')
            st.markdown('''### *:red[Últimas notícias] :red[..] :red[.]*''')
            st.write('')
            if articles:
                for article in articles:
                    if 'urlToImage' in article and article['urlToImage']:  # Verifica se a imagem existe
                        st.image(article['urlToImage'], caption=article['title'], width=270)
                    st.markdown("##### " + article["description"])
                    st.write("Fonte:", article["source"]["name"])
                    published_date = datetime.datetime.strptime(article["publishedAt"], '%Y-%m-%dT%H:%M:%SZ')  # Converte para datetime
                    formatted_date = published_date.strftime('%d/%m/%Y')  # Formata como dia/mês/ano
                    st.markdown(f"Publicado em: {formatted_date}")
                    st.write("[Leia mais](" + article["url"] + ")")

        with tab1:
            self.analise_diaria()
        with tab2:
            self.rendimento()
        with tab3:
            self.variacao()
        with tab4:
            self.volume()
        with tab5:
            self.dividendo()
        with tab6:
            self.vender()
        with tab7:
            tab1, tab2, tab3, tab4 = st.tabs(['CriptoMoedas', 'Ações', 'Exterior', 'ETFs'])
            with tab1:
                articles = get_noticias("criptomoedas")
                if articles:
                    show_news(articles)
                else:
                    st.write("Não foi possível carregar as notícias. Tente novamente mais tarde.")
            
            with tab2:
                articles = get_noticias("Ibovespa")
                if articles:
                    show_news(articles)
                else:
                    st.write("Não foi possível carregar as notícias. Tente novamente mais tarde.")
            
            with tab3:
                articles = get_noticias("nasdaq, dow jones e S&P500")
                if articles:
                    show_news(articles)
                else:
                    st.write("Não foi possível carregar as notícias. Tente novamente mais tarde.")

            with tab4:
                articles = get_noticias("Exchange-Traded Funds")
                if articles:
                    show_news(articles)
                else:
                    st.write("Não foi possível carregar as notícias. Tente novamente mais tarde.")                

    def display_data(self):
        df_acoes = get_acoes()
        df_acoes['Date'] = pd.to_datetime(df_acoes['Date']).dt.date
        df_fundos = get_fundos()
        df_name_fundos = get_name_fundos()
        df_cdi = get_cdi()
        df_dolar = get_dolar()

        base_fundos = pd.merge(df_fundos, df_name_fundos, how = "left", 
                            left_on = ["CNPJ_FUNDO"], right_on = ["CNPJ_FUNDO"])
        base_fundos = base_fundos[['CNPJ_FUNDO', 'DENOM_SOCIAL', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST']]
        base_multimercado = base_fundos.rename(columns={'DENOM_SOCIAL': 'Symbol', 'DT_COMPTC': 'Date', 'VL_QUOTA': 'Close'})

        # Concatenar base_multimercado e df_cdi
        base_multimercado = pd.concat([base_multimercado, df_cdi, df_dolar], ignore_index=True)

        # base_multimercado = base_multimercado[base_multimercado['Symbol'].str.contains("ARMOR AXE FI|ABSOLUTE HIDRACDI", na = False)]
    
        # Substituir os valores na coluna 'nome_fundo'
        base_multimercado['Symbol'] = base_multimercado['Symbol'].replace('ARMOR AXE FI EM COTAS DE FUNDOS DE INVESTIMENTO MULTIMERCADO', 'ARMOR AXE')
        base_multimercado['Symbol'] = base_multimercado['Symbol'].replace('ABSOLUTE HIDRA CDI FIC DE FIF RENDA FIXA INVESTIMENTO EM INFRAESTRUTURA CRÉDITO PRIVADO - RL', 'ABSOLUTE HIDRA')
        base_multimercado['Symbol'] = base_multimercado['Symbol'].replace('ITAÚ AÇÕES BDR NÍVEL I FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO', 'ITAÚ FUNDOS')
        base_multimercado['Symbol'] = base_multimercado['Symbol'].replace('ITAÚ INDEX US TECH FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM AÇÕES', 'US TECH')

        # Concatenando os DataFrames verticalmente
        df = pd.concat([df_acoes, base_multimercado], ignore_index=True)

        st.title('Genius Strategy')
        
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        # Adiciona o slider para selecionar o intervalo de datas
        # dates = df['Date'].unique()

        # self.inicio_data, self.fim_data = st.select_slider(
        #                                         "Selecione o intervalo de datas",
        #                                         options=dates,
        #                                         value=(dates.min(), dates.max())
        #                                         )
        
        # datas que inicia o sistema
        tempo = time.time()
        tempo_local = time.localtime(tempo)

        col1, col2 = st.columns(2)
        with col1:
            self.inicio_data = st.date_input(
                'Data inicial', 
                datetime.date(tempo_local[0], tempo_local[1], 1), format='DD/MM/YYYY')
        with col2:
            self.fim_data = st.date_input(
                    'Data final', 
                    datetime.date.today(), format='DD/MM/YYYY')

        # Filtro por Symbol
        selecao = st.radio('Seleção',
                                ['Top5 + Minhas Ações', 'Acompanhando', 'Top5', 'Minhas Ações', 'MultiMercado',
                                    'Fundo', 'Exterior', 'CriptoMoeda'], horizontal=True, index=2)
        
        # Obter os símbolos disponíveis no DataFrame
        simbolos = df['Symbol'].unique()

        top5_itau = ['CXSE3.SA', 'PRIO3.SA', 'DIRR3.SA', 'EQTL3.SA', 'SANB11.SA']
        minha_acoes = ['ALUP11.SA', 'CPLE6.SA', 'BBAS3.SA', 'CYRE3.SA', 'ITUB4.SA', 'VIVA3.SA']
        multimercado = ['ARMOR AXE', 'ABSOLUTE HIDRA']
        acompanhando = ['PETR4.SA', 'VALE3.SA', 'GMAT3.SA', 'IGTI11.SA', 'SUZB3.SA', 'WEGE3.SA']
        fundos = ['US TECH', 'ITAÚ FUNDOS']
        cripto_moeda = [# Baixo risco
                        'BTC-USD', 'ETH-USD',
                        # Médio risco
                        'LINK-USD', 'TON-USD', 'ATOM-USD', 'SOL-USD',  'AVAX-USD', 'ARB-USD', 'OP-USD',
                        # Alto risco    
                        'APT-USD', 'SUI20947-USD', 'LDO-USD'
                        ]

        exterior = ['VOO', 'QQQ', 'ACWI']

        if selecao == 'Top5 + Minhas Ações':
            default_selecao = minha_acoes + top5_itau
        elif selecao == 'Acompanhando':
            default_selecao = acompanhando
        elif selecao == 'Minhas Ações':
            default_selecao = minha_acoes
        elif selecao == 'MultiMercado':
            default_selecao = multimercado
        elif selecao == 'Exterior':
            default_selecao = exterior
        elif selecao == 'Fundo':
            default_selecao = fundos
        elif selecao == 'CriptoMoeda':
            default_selecao = cripto_moeda
        else:
            default_selecao = top5_itau

        # Garantir que os valores de selecao estão nas opções disponíveis
        default_selecao = [item for item in default_selecao if item in simbolos]

        self.select_symbol = st.multiselect('Selecione as ações', 
                                       simbolos, 
                                       default=default_selecao,
                                       placeholder='Escolha uma opção')
        if self.select_symbol:
            df = df[df['Symbol'].isin(self.select_symbol)]

        # Nesse parte eu pego a data exata da seleção
        # Filtra o DataFrame com base no intervalo de datas selecionado
        # mask = (df['Date'] >= self.inicio_data) & (df['Date'] <= self.fim_data)
        # self.filtered_df = df.loc[mask]

        # Encontra o último dia útil antes da `inicio_data`
        ultimo_dia_util = df[df['Date'] < self.inicio_data].Date.max()

        # Cria uma máscara que inclui o último dia útil e o intervalo de datas selecionado
        # estou pegando um dia útil anterior para que o calculo do rendimento fica correto
        mask = ((df['Date'] >= ultimo_dia_util) & (df['Date'] <= self.fim_data))
        self.filtered_df = df.loc[mask]
        
        # Verifique quantos símbolos únicos estão presentes no DataFrame filtrado
        self.unique_symbols = self.filtered_df['Symbol'].unique()

        if len(self.unique_symbols) > 1:
            # Se houver mais de um símbolo, use pivot_table para reestruturar o DataFrame
            self.pivot_df = self.filtered_df.pivot_table(index='Date', columns='Symbol', values='Close')
        else:
            # Se houver apenas um símbolo, mantenha o DataFrame como está
            self.pivot_df = self.filtered_df.set_index('Date')[['Close']]

        # Ordene o DataFrame pelo índice 'Date'
        self.pivot_df = self.pivot_df.sort_values(by='Date')

    def card(self):
        df = get_acoes()
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        mask = (df['Date'] >= self.inicio_data) & (df['Date'] <= self.fim_data)
        df = df.loc[mask]

        # Encontrar o valor máximo da coluna 'Close' para cada 'Symbol'
        max_close_symbol = df.groupby('Symbol')['Close'].last()

        # Encontrar o último valor da coluna 'Variação' para cada 'Symbol'
        last_variation_symbol = df.groupby('Symbol')['Variação'].last()

        # Encontrar o símbolo com a maior variação
        symbol_max_variation = last_variation_symbol.idxmax() if not last_variation_symbol.empty else "Nenhum destaque"

        # Supondo que você tenha um seletor de símbolos (self.select_symbol)
        if self.select_symbol:
            # Filtrar os dados conforme a seleção feita
            filtered_symbols = [symbol for symbol in self.select_symbol if symbol in max_close_symbol.index]

            # Criar o número correto de colunas
            # cols = st.columns(len(filtered_symbols) + 2)  # Adiciona mais duas colunas para os cartões fixos
            cols = st.columns(len(filtered_symbols) + 1)
            for i, symbol in enumerate(filtered_symbols):
                with cols[i]:
                    ui.metric_card(
                        title=symbol,
                        content=round(max_close_symbol[symbol], 2),
                        description=f"{round(last_variation_symbol[symbol], 2)}% Variação",
                        key=f"card{i+1}"
                    )

            # Adiciona o cartão fixo para "Fundo Imob"
            # with cols[len(filtered_symbols)]:
            #     ui.metric_card(
            #         title="ITUB4.SA",
            #         content=max_close_symbol['ITUB4.SA'].round(2),
            #         description=f"{last_variation_symbol['ITUB4.SA'].round(2)}% Variação",
            #         key="card77"
            #     )

            # Calcular o valor dinâmico do fechamento com base nos símbolos filtrados
            filtered_variations = last_variation_symbol[filtered_symbols]
            # a linha abaixo deixava ações da ITUB fixa
            # fechamento_value = last_variation_symbol[filtered_symbols].sum() + last_variation_symbol['ITUB4.SA'] if filtered_symbols else 0
            fechamento_value = last_variation_symbol[filtered_symbols].sum() if filtered_symbols else 0

            # Encontrar o símbolo com a maior variação entre os símbolos filtrados
            symbol_max_variation_filtered = filtered_variations.idxmax() if not filtered_variations.empty else "Nenhum destaque"

            # Adiciona o cartão dinâmico para "Fechamento"
            # with cols[len(filtered_symbols) + 1]:
            with cols[len(filtered_symbols)]:
                ui.metric_card(
                    title="Fechamento",
                    content=round(fechamento_value, 2),
                    description=f'Destaque {symbol_max_variation_filtered}',
                    key="card88"
                )

    def analise_diaria(self):
        self.table_geral = self.filtered_df.copy()

        # col1, col2 = st.columns([1, 0.5])
        # col1, col2, col3, col4 = st.columns([1.5, 1, 0.32, 0.38])
        col1, col2, col3 = st.columns([1.25, 0.7, 0.25])
        with col1:
            # Use st.line_chart para criar o gráfico de linhas
            st.line_chart(self.pivot_df)

        with col2:
            df_dia_agrupado = self.table_geral.groupby(['Date'])['Variação'].sum().reset_index()
            dias_positivos = (df_dia_agrupado['Variação'] >= 0).sum()
            dias_negativos = (df_dia_agrupado['Variação'] < 0).sum()
            st.markdown(f':blue[{dias_positivos}] dias no positivo e *:red[{dias_negativos}]* dias negativo')
            
            self.table_geral = self.table_geral.sort_values(by='Date', ascending=False)
            self.table_geral = self.table_geral.drop('Dividends', axis=1)

            # Calcular o rendimento para cada linha
            def calcular_rendimento_linha(linha, df):
                symbol = linha['Symbol']
                close_atual = linha['Close']
                data_atual = linha['Date']
                
                if symbol == 'CDI':
                    # rendimento = df[df['Symbol'] == 'CDI']['Close'].cumsum().iloc[-1]
                    # Filtrar o DataFrame para o CDI até a data atual
                    rendimento = df[(df['Symbol'] == 'CDI') & (df['Date'] <= data_atual)]['Close'].cumsum().iloc[-1]

                else:
                    # Filtrar o DataFrame para o mesmo símbolo e buscar a menor data
                    menor_data = df[df['Symbol'] == symbol]['Date'].min()
                    close_menor_data = df[(df['Symbol'] == symbol) & (df['Date'] == menor_data)]['Close'].values[0]
                    # Calcular o rendimento
                    rendimento = ((close_atual - close_menor_data) / close_menor_data * 100).round(2)
                return rendimento
            # Aplicar a função para cada linha
            self.table_geral['Rendimento'] = self.table_geral.apply(calcular_rendimento_linha, axis=1, df=self.table_geral)

            st.dataframe(self.table_geral, hide_index=True, column_order=['Date', 'Symbol', 'Open', 'Low', 'Close', 'Variação', 'Rendimento'])

        with col3:
            acumulado = self.table_geral['Variação'].sum().round(2)
            st.markdown(f'Variação {acumulado}')

            df_symbol_agrupado = self.table_geral.groupby(['Symbol'])['Variação'].sum()
            st.dataframe(df_symbol_agrupado, use_container_width=True)

        # with col4:
        #     ultima_data = self.table_geral['Date'].max()
        #     df_rendimento = self.table_geral[self.table_geral['Date'] == ultima_data]
        #     df_rendimento = df_rendimento.groupby(['Symbol'])['Rendimento'].sum()

        #     # a soma foi feita diferente devido df_rendimento ter se tornado uma Series do pandas
        #     rendimento_symbol = (df_rendimento[:].sum() / len(df_rendimento[0:])).round(2)
        #     st.markdown(f'Crescimento {rendimento_symbol}%')

        #     st.dataframe(df_rendimento)

    def variacao(self):
        if len(self.unique_symbols) > 1:
            # Se houver mais de um símbolo, use pivot_table para reestruturar o DataFrame
            pivot_df_variacao = self.filtered_df.pivot_table(index='Date', columns='Symbol', values='Variação')
        else:
            # Se houver apenas um símbolo, mantenha o DataFrame como está
            pivot_df_variacao = self.filtered_df.set_index('Date')[['Variação']]

        # Ordene o DataFrame pelo índice 'Date'
        pivot_df_variacao = pivot_df_variacao.sort_values(by='Date')
        
        # Calculate the rolling mean with a window of 30 days
        pivot_df_variacao['Média Móvel'] = pivot_df_variacao.mean(axis=1).rolling(window=30).mean()
        pivot_df_variacao['Linha 0'] = 0
        st.line_chart(pivot_df_variacao)

        st.write('Variação acumulada por Período')
        variacao_total = pivot_df_variacao.copy()
        variacao_total['Total'] = variacao_total.loc[:, :].sum(axis=1)
        st.line_chart(variacao_total['Total'], color='#FFBF00')

        st.write('Variação acumulada por Symbol')
        variação_symbol = pivot_df_variacao.drop(['Média Móvel', 'Linha 0'], axis=1)
        variação_symbol = variação_symbol.sum(axis=0)
        st.line_chart(variação_symbol, color='#39FF14')


        def grafico_com_altair():
            df = self.filtered_df.copy()

            df['Média 30d'] = pivot_df_variacao.mean(axis=0).rolling(window=30).mean()

            # Calculate the rolling mean
            df['Média 30d'] = df.groupby('Symbol')['Variação'].transform(lambda x: x.rolling(window=30).mean())

            # Create the chart
            chart = alt.Chart(df).mark_line().encode(
                x='Date:T',
                y='Variação:Q',
                color='Symbol:N',   
                tooltip=['Date:T', 'Variação:Q']
            ) + alt.Chart(pd.DataFrame({'y': [0]})).mark_rule(color='red').encode(
                y='y:Q'
            )

            # Create the rolling mean line
            rolling_mean_line = alt.Chart(df).mark_line(size=1.3, opacity=0.9, color='orange').encode(
                x='Date:T',
                y='Média 30d:Q',
                tooltip=['Média 30:T']
            )

            # Combine all layers
            final_chart = chart + rolling_mean_line

            # Customize the legend position
            final_chart = final_chart.configure_legend(
                        orient='bottom',  # move the legend to the bottom
                        legendX=0,  # align the legend to the left
                        legendY=0,  # align the legend to the top (within the bottom area)
                        titleOrient='left',  # align the legend title to the left
                        title=None
                        )

            # Display the chart in Streamlit
            st.altair_chart(final_chart, use_container_width=True)

    def volume(self):
        if len(self.unique_symbols) > 1:
            # Se houver mais de um símbolo, use pivot_table para reestruturar o DataFrame
            pivot_df_volume = self.filtered_df.pivot_table(index='Date', columns='Symbol', values='Volume')
        else:
            # Se houver apenas um símbolo, mantenha o DataFrame como está
            pivot_df_volume = self.filtered_df.set_index('Date')[['Volume']]

        # Ordene o DataFrame pelo índice 'Date'
        pivot_df_volume = pivot_df_volume.sort_values(by='Date')

        st.line_chart(pivot_df_volume)        

    def dividendo(self):
        df = self.filtered_df
        df = df.drop(['Open', 'High', 'Low', 'Close', 'Volume', 'Variação'], axis=1)
        df = df[df['Dividends'] > 0]
        
        cols = st.columns([1.75, 0.25])
        with cols[0]:
            st.bar_chart(data=df, x='Date', y='Dividends', color='Symbol', height=400, use_container_width=True)
        with cols[1]:
            df_dividendo_acum = df.groupby(['Symbol'])['Dividends'].sum()
            st.write('Dividendos Acumulado')
            ui.table(data=df_dividendo_acum.reset_index())        

        # Criar o gráfico de barras usando Altair
        # chart = alt.Chart(df).mark_bar(size=15).encode(
        #     x='Date:T',
        #     y='Dividends:Q',
        #     color='Symbol:N',
        #     tooltip=['Date', 'Symbol', 'Dividends'],
        # ).properties(
        #     width=800,
        #     height=400
        # ).interactive()

        # # Exibir o gráfico no Streamlit
        # st.altair_chart(chart, use_container_width=True)

    def rendimento(self):
        df = self.filtered_df.copy()
        col1, col2 = st.columns([1.7, 0.28])
        # col1, col2, col3 = st.columns([2.4, 0.33, 0.38])
        with col1:
            crescimento = self.table_geral.groupby(['Date'])['Rendimento'].mean()
            st.write('Rendimento diário do Período')
            st.line_chart(crescimento, color='#FFBF00')

            if len(self.unique_symbols) > 1:
            # Se houver mais de um símbolo, use pivot_table para reestruturar o DataFrame
                pivot_df_variacao = self.table_geral.pivot_table(index='Date', columns='Symbol', values='Rendimento')
            else:
                # Se houver apenas um símbolo, mantenha o DataFrame como está
                pivot_df_variacao = self.table_geral.set_index('Date')[['Rendimento']]

            # Ordene o DataFrame pelo índice 'Date'
            pivot_df_variacao = pivot_df_variacao.sort_values(by='Date')
            
            # Calculate the rolling mean with a window of 30 days
            pivot_df_variacao['Média Móvel'] = pivot_df_variacao.mean(axis=1).rolling(window=30).mean()
            pivot_df_variacao['Linha 0'] = 0
            st.write('Rendimento diário por Symbol')
            st.line_chart(pivot_df_variacao)

            st.write('Rendimento acumulada por Symbol')
            rendimento_symbol = pivot_df_variacao.drop(['Média Móvel', 'Linha 0'], axis=1)
            # rendimento_symbol = rendimento_symbol.mean(axis=0)
            rendimento_symbol = rendimento_symbol.iloc[-1]
            st.line_chart(rendimento_symbol, color='#39FF14')

            # Calcular o rendimento para cada linha
            def calcular_rendimento_linha(linha, df):
                symbol = linha['Symbol']
                close_atual = linha['Close']
                # Filtrar o DataFrame para o mesmo símbolo e buscar a menor data
                menor_data = df[df['Symbol'] == symbol]['Date'].min()
                close_menor_data = df[(df['Symbol'] == symbol) & (df['Date'] == menor_data)]['Close'].values[0]
                # Calcular o rendimento
                rendimento = ((close_atual - close_menor_data) / close_menor_data * 100).round(2)
                return rendimento
            
            # Aplicar a função para cada linha
            df['Rendimento'] = df.apply(calcular_rendimento_linha, axis=1, df=df)

            st.write("Rendimento diário")
            st.dataframe(pivot_df_variacao.drop(['Média Móvel', 'Linha 0'], axis=1))
           
        # Tabela de rendimento
        # Função para preparar os dados iniciais
        def preparar_dados(df):
            df['Date'] = pd.to_datetime(df['Date'])
            df['Mês/Ano'] = df['Date'].dt.to_period('M').dt.strftime('%b/%y')
            return df

        # Função para calcular rendimento acumulativo
        def calcular_rendimento_acumulativo(df):
            rendimento_acumulativo = (
                df.groupby(['Symbol', df['Date'].dt.to_period('M')])['Rendimento']
                .last()
                .reset_index()
                .dropna()
            )
            rendimento_acumulativo['Date'] = rendimento_acumulativo['Date'].dt.to_timestamp()
            rendimento_acumulativo['Mês/Ano'] = rendimento_acumulativo['Date'].dt.strftime('%b/%y')
            return rendimento_acumulativo.pivot(
                index='Symbol', columns='Mês/Ano', values='Rendimento'
            ).sort_index(axis=1, key=lambda x: pd.to_datetime(x, format='%b/%y'))

        # Função para calcular rendimento mensal
        def calcular_rendimento_mensal(df):
            rendimento_mensal = (
                df.groupby(['Symbol', df['Date'].dt.to_period('M')])['Rendimento']
                .last()
                .reset_index()
                .dropna()
            )
            rendimento_mensal['Rendimento_Mensal'] = rendimento_mensal.groupby('Symbol')['Rendimento'].diff()
            rendimento_mensal['Date'] = rendimento_mensal['Date'].dt.to_timestamp()
            rendimento_mensal['Mês/Ano'] = rendimento_mensal['Date'].dt.strftime('%b/%y')
            return rendimento_mensal.pivot(
                index='Symbol', columns='Mês/Ano', values='Rendimento_Mensal'
            ).sort_index(axis=1, key=lambda x: pd.to_datetime(x, format='%b/%y'))

        # Preparar os dados
        df = preparar_dados(df)

        # Calcular rendimentos
        df_rendimento_acumulativo = calcular_rendimento_acumulativo(df)
        df_rendimento_mensal = calcular_rendimento_mensal(df)

        # Exibir no Streamlit
        st.write("Rendimento Acumulativo")
        st.dataframe(df_rendimento_acumulativo)

        st.write("Rendimento Mensal")
        st.dataframe(df_rendimento_mensal)

        with col2:
            ultima_data = df['Date'].max()
            df_rendimento = df[df['Date'] == ultima_data]
            df_rendimento = df_rendimento.groupby(['Symbol'])['Rendimento'].sum()

            # a soma foi feita diferente devido df_rendimento ter se tornado uma Series do pandas
            rendimento_symbol = (df_rendimento[:].sum() / len(df_rendimento[0:])).round(2)
            st.markdown(f'Crescimento {rendimento_symbol}%')
            st.dataframe(df_rendimento, use_container_width=True)

    def vender(self):
        # esse copy esta me trazendo apenas as colunas selecionadas dispensando o drop
        df_vendas = self.table_geral[['Date', 'Symbol']].copy()
        df_vendas = self.table_geral[self.table_geral['Date'] == self.table_geral['Date'].max()]
        
        # gerando dicionario com valores de venda
        valor_venda = {
            'Symbol': ['ALUP11.SA', 'CPLE6.SA', 'BBAS3.SA', 'CYRE3.SA', 'ITUB4.SA', 'VIVA3.SA',
                        'BTC-USD', 'ETH-USD', 'LINK-USD', 'TON-USD', 'ATOM-USD', 'SOL-USD',
                        'AVAX-USD', 'ARB-USD', 'APT-USD', 'LDO-USD', 'SUI20947-USD', 'OP-USD', 'ME-USD'],
            'Valor Venda': [42.7, 13.30, 31.00, 30.00, 0.00, 32.00,
                            119594.41, 4758.36, 30.77, 8.05, 11.98, 282.59,
                            63.36, 1.37, 17.06, 2.57, 4.96, 3.14, 6.52
                            ],
            'Valor Compra': [31.57, 10.67, 28.35, 22.25, 36.57, 27.07,
                              99662, 3965.3, 25.64, 6.71, 9.98, 235.49,
                              52.80, 1.14, 14.22, 2.14, 4.13, 2.62, 5.43
                              ],
            'Data Compra': ['2024-08-26', '2024-08-26', '2024-08-26', '2024-08-27', '2024-08-23', '2024-08-26',
                            '2024-12-08', '2024-12-08', '2024-12-08', '2024-12-08', '2024-12-08', '2024-12-08',
                            '2024-12-08', '2024-12-08', '2024-12-08', '2024-12-08', '2024-12-08', '2024-12-08', '2024-12-10']
            }        
        df_valor_venda = pd.DataFrame(valor_venda)

        df_vendas = pd.merge(df_vendas, df_valor_venda, on='Symbol', how='inner')
        df_vendas['Rentabilidade'] = ((df_vendas['Close'] - df_vendas['Valor Compra']) / df_vendas['Valor Compra'] * 100).round(2)
        df_vendas = df_vendas.sort_values(by='Rendimento', ascending=False)
        if df_vendas.empty:
            st.warning("DataFrame vazio.")
        else:
            st.dataframe(df_vendas, 
                                hide_index=True, 
                                column_config={'Rentabilidade': st.column_config.NumberColumn('Rentabilidade', format='%.2f %%')},
                                column_order=['Data Compra', 'Date', 'Symbol', 'Valor Compra', 'Close', 'Valor Venda', 'Rentabilidade'])

if __name__ == "__main__":
    Application()
