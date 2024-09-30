import access as access
import pandas as pd
import matplotlib.pyplot as plt
import random
from jinja2 import Environment, FileSystemLoader
from decimal import Decimal

template_dir = '/pyspark/templates'
template_file = 'template.html'
template_encoding = 'utf-8'
report_encoding = 'utf-8'
report_file_path = '/pyspark/reports/relatorio_enem_2020.html'
img_dir = '/pyspark/reports/img'


def run_query(query):
    # Executar a query no banco de dados
    access.cursor.execute(query)

    # Recuperar os resultados
    result = access.cursor.fetchall()

    # Recuperar o nome das colunas e criar um dataframe Pandas
    # com os dados recuperados. Caso não haja resultado, retorna
    # um dataframe vazio
    columns = [column[0] for column in access.cursor.description]
    if result:
        result_list = [list(row) for row in result]
        return pd.DataFrame(result_list, columns=columns)
    else:
        return pd.DataFrame()  


def generate_bar_chart(df, file):
    # Gera cores aleatórias para os elementos do gráfico
    colors = ['#' + ''.join(random.choices('0123456789ABCDEF', k=6)) for i in range(len(df))]

    # Define o tamanho da figura
    plt.figure(figsize=(10, 6))

    # Usa o nome das colunas do dataframe para nomear os elementos do gráfico
    bars = plt.bar(df[df.columns[0]], df[df.columns[1]], color=colors) 
    
    plt.xlabel(df.columns[0])
    plt.ylabel(df.columns[1])
    plt.xticks(rotation=45)
    plt.grid(axis='y')

    # Insere o valor numérico acima das barras
    for bar in bars:
        yval = bar.get_height()

        # O valor numérico será impresso com apenas 1 casa decimal
        # e sem arredondamento
        plt.text(bar.get_x() + bar.get_width()/2, yval, f"{int(yval * 10) / 10:.1f}", ha='center', va='bottom')

    plt.tight_layout()

    # Salva o gráfico no arquivo especificado
    plt.savefig(f'{img_dir}/{file}')
    
    plt.close()

    # Retorna o nome do arquivo
    return file


def generate_pie_chart(values_with_labels, file):
    # Define o tamanho do gráfico
    plt.figure(figsize=(8, 6))

    # Recupera os valores e legendas e os insere no gráfico.
    # values_with_labels[0] --> Valores
    # values_with_labels[1] --> Legendas
    plt.pie(values_with_labels[0], labels=values_with_labels[1], 
            autopct=lambda p: f'{p:.2f}%', 
            startangle=140, 
            # Cores aleatórias
            colors=['#' + ''.join(random.choices('0123456789ABCDEF', k=6)) for i in range(len(values_with_labels[0]))])
    
    # Faz com que o gráfico tome forma de círculo exato
    plt.axis('equal')

    # Salva o gráfico no arquivo especificado
    plt.savefig(f'{img_dir}/{file}', bbox_inches='tight')
    
    plt.close()

    # Retorna o nome do arquivo gerado
    return file


# Os métodos abaixo são correspondentes a cada pergunta
# contida no relatório
def question1():
    question = '1 - Qual a escola com a maior média de notas?'
    image_name = 'media_por_tipo_escola.png'
    query = '''
SELECT 
	CASE 
		WHEN a.tp_escola = '1' THEN 'Não Respondeu'
		WHEN a.tp_escola = '2' THEN 'Pública'
		WHEN a.tp_escola = '3' THEN 'Privada'
		WHEN a.tp_escola = '4' THEN 'Exterior'
	END AS 'Tipo de escola',
	AVG(nu_media) AS "Média"
FROM dw_enem_2020.ft_notas n
INNER JOIN dw_enem_2020.dim_candidato_inscrito a ON n.nu_inscricao = a.nu_inscricao
GROUP BY a.tp_escola
'''
    result = run_query(query)

    # Geração da imagem do gráfico
    image = generate_bar_chart(result, image_name)

    # Das médias recuperadas, pegar a linha
    # correspondente a de maior valor
    max = result.loc[result['Média'].idxmax()]

    # Resposta textual com valor da média formatado
    # com 1 casa decimal e sem arredondamento
    response = f'O tipo de escola com maior média é: {max["Tipo de escola"]}<br>' + \
    f'O valor da média é: {int(max["Média"] * 10) / 10:.1f}'

    return {'question': question,
            'response': response,
            'image': image}


def question2():
    question = '2 - Qual o aluno com a maior média de notas e o valor dessa média?'
    query = '''
WITH
	maior_media AS
	(
		SELECT MAX(nu_media) AS 'Maior média'
		FROM dw_enem_2020.ft_notas n 
	)
SELECT nu_inscricao AS "Número de inscrição do aluno", nu_media AS "Média"
FROM dw_enem_2020.ft_notas n
WHERE nu_media = (SELECT * FROM maior_media)
'''
    result = run_query(query)

    # Resposta textual com valor da média formatado
    # com 1 casa decimal e sem arredondamento
    response = f'O número de inscrição do aluno com a maior média é {result.loc[0, "Número de inscrição do aluno"]} e sua nota foi ' + \
        f'{int(result.loc[0, "Média"] * 10) / 10:.1f}'
    return {'question': question,
            'response': response}


def question3():
    question = '3 - Qual a média geral?'
    query = '''
SELECT AVG(nu_media) AS "Média geral" 
FROM dw_enem_2020.ft_notas
'''
    result = run_query(query)

    # Resposta textual com valor da média formatado
    # com 1 casa decimal e sem arredondamento
    response = f'A média geral do ENEM 2020 foi: {int(result.loc[0, "Média geral"] * 10) / 10:.1f}'
    return {'question': question,
            'response': response}


def question4():
    question = '4 - Qual o % de Ausentes?'
    image_name = 'percentual_ausentes.png'
    query = '''
WITH
	qtd_candidatos_totais AS
	(
		SELECT COUNT(nu_inscricao) AS "Quantidade"
		FROM dw_enem_2020.dim_candidato_inscrito
	),
	qtd_candidatos_ausentes AS
	(
		SELECT COUNT(nu_inscricao) AS "Quantidade"
		FROM dw_enem_2020.dim_candidato_inscrito
		WHERE
			tp_presenca_cn = 0
			AND tp_presenca_ch = 0
			AND tp_presenca_lc = 0
			AND tp_presenca_mt = 0
			-- AND tp_status_redacao = 4
	)
SELECT 
	(((SELECT Quantidade FROM qtd_candidatos_ausentes) * 1.0) /
	((SELECT Quantidade FROM qtd_candidatos_totais) * 1.0)) * 100 AS "Canditatos ausentes (%)"
'''
    result = run_query(query)

    # Resposta textual com valor percentual formatado
    # com 2 casas decimais e sem arredondamento
    response = f'A abstenção no ENEM 2020 foi de: {int(result.loc[0, "Canditatos ausentes (%)"] * 100) / 100} %'

    # Tupla com primeiro elemento correspondendo ao
    # percentual de candidatos ausentes e o segundo
    # elemento ao de candidatos presentes (calculado
    # através de 100.00 - percentual de ausentes)
    values_with_labels = ([result['Canditatos ausentes (%)'].values[0], 
                           Decimal(100.00) - result['Canditatos ausentes (%)'].values[0]], 
                           ['Ausentes', 'Presentes'])
    
    # Geração da imagem do gráfico
    image = generate_pie_chart(values_with_labels=values_with_labels, file=image_name)
    
    return {'question': question,
            'response': response,
            'image': image}


def question5():
    question = '5 - Qual o número total de Inscritos?'
    query = '''
SELECT COUNT(nu_inscricao) AS "Quantidade de inscritos"
FROM dw_enem_2020.dim_candidato_inscrito
'''
    result = run_query(query)
    response = f'A quantidade de inscritos no ENEM 2020 foi de: {result.loc[0, "Quantidade de inscritos"]}'
    return {'question': question,
            'response': response}


def question6():
    question = '6 - Qual a média por disciplina?'
    image_name = 'media_por_disciplina.png'
    query = '''
SELECT
	'Ciencias da natureza' AS "Matéria",
	AVG(nu_nota_cn) AS "Média"
FROM dw_enem_2020.ft_notas
UNION
SELECT
	'Ciências humanas' AS "Matéria",
	AVG(nu_nota_ch) AS "Média"
FROM dw_enem_2020.ft_notas
UNION
SELECT
	'Linguagens e códigos' AS "Matéria",
	AVG(nu_nota_lc) AS "Média"
FROM dw_enem_2020.ft_notas
UNION
SELECT
	'Matemática' AS "Matéria",
	AVG(nu_nota_mt) AS "Média"
FROM dw_enem_2020.ft_notas
'''
    result = run_query(query)
    
    # Geração da resposta com os valores das médias
    # em cada linha formatados com 1 casa decimal e
    # sem arredondamento
    response = ''
    for row in result.itertuples(index=True):
        response += f'{row[1]}: {f"{int(row[2] * 10) / 10:.1f}"}<br>'
    
    # Geração da imagem do gráfico
    image = generate_bar_chart(result, image_name)
    
    return {'question': question,
            'response': response,
            'image': image}



def question7():
    question = '7 - Qual a média por Sexo?'
    image_name = 'media_por_sexo.png'
    query = '''
SELECT
	CASE 
		WHEN a.tp_sexo = 'M' THEN 'Masculino'
		WHEN a.tp_sexo = 'F' THEN 'Feminino'
	END AS "Sexo", 
	AVG(nu_media) AS "Média"
FROM dw_enem_2020.ft_notas n
INNER JOIN dw_enem_2020.dim_candidato_inscrito a ON n.nu_inscricao = a.nu_inscricao
GROUP BY a.tp_sexo
ORDER BY "Sexo" ASC
'''
    print()
    result = run_query(query)
    
    # Geração da resposta com os valores das médias
    # em cada linha formatados com 1 casa decimal e
    # sem arredondamento
    response = ''
    for row in result.itertuples(index=True):
        response += f'{row[1]}: {f"{int(row[2] * 10) / 10:.1f}"}<br>'
    
    # Geração da imagem do gráfico
    image = generate_bar_chart(result, image_name)
    
    return {'question': question,
            'response': response,
            'image': image}


def question8():
    question = '8 - Qual a média por Etnia?'
    image_name = 'media_por_etnia.png'
    query = '''
SELECT
	CASE 
		WHEN a.tp_cor_raca = 0 THEN 'Não declarado'
		WHEN a.tp_cor_raca = 1 THEN 'Branca'
		WHEN a.tp_cor_raca = 2 THEN 'Preta'
		WHEN a.tp_cor_raca = 3 THEN 'Parda'
		WHEN a.tp_cor_raca = 4 THEN 'Amarela'
		WHEN a.tp_cor_raca = 5 THEN 'Indígena'
	END AS "Etnia", 
	AVG(nu_media) AS "Média"
FROM dw_enem_2020.ft_notas n
INNER JOIN dw_enem_2020.dim_candidato_inscrito a ON n.nu_inscricao = a.nu_inscricao
GROUP BY a.tp_cor_raca
ORDER BY "Etnia" ASC
'''
    result = run_query(query)
    
    # Geração da resposta com os valores das médias
    # em cada linha formatados com 1 casa decimal e
    # sem arredondamento
    response = ''
    for row in result.itertuples(index=True):
        response += f'{row[1]}: {f"{int(row[2] * 10) / 10:.1f}"}<br>'
    
    # Geração da imagem do gráfico
    image = generate_bar_chart(result, image_name)
    
    return {'question': question,
            'response': response,
            'image': image}


def main():
    # Título do relatório
    title = 'Relatório ENEM 2020'

    # Lista com as questões do relatório
    # devidamente respondidas
    questions = [question1(),
                 question2(),
                 question3(),
                 question4(),
                 question5(),
                 question6(),
                 question7(),
                 question8()]
    
    # Renderizar o template
    env = Environment(loader=FileSystemLoader(template_dir, encoding=template_encoding))
    template = env.get_template(template_file)
    html_content = template.render(title=title, questions=questions)

    with open(report_file_path, 'w', encoding=report_encoding) as f:
        f.write(html_content)

if __name__ == "__main__":
    main()
    