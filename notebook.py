# Célula 1
from pyspark.sql import functions as F

# Leitura do arquivo
fut_players = spark.read.csv(
    "/Volumes/workspace/default/atividade_1/fut_players_data.csv",
    header=True,
    inferSchema=True
)

# Visualização das 5 primeiras linhas
display(fut_players.limit(5))

# Célula 2
# Filtra os jogadores com dribbling e shooting acima de 90
the_best = (
    fut_players
    .select(
        'player_id', 'player_name', 'nationality',
        'position', 'dribbling', 'shooting', 'overall'
    )
    .where(
        (F.col('dribbling') > 90) &
        (F.col('shooting') > 90)
    )
)

# DataFrame auxiliar com as nacionalidades
nationalities = fut_players.select(
    'player_id', 'player_name', 'nationality', 'position'
)

# Junta os jogadores com suas nacionalidades usando player_id
the_best_nationality = the_best.join(
    nationalities,
    ['player_id'],
    how="left"
)

display(the_best_nationality)

# Célula 3
# Classificação dos jogadores de acordo com o overall
fut_players_classified = (
    fut_players
    .select("player_id", "overall")
    .withColumn(
        "classification",
        F.when(F.col("overall") <= 50, "Amador")
         .when(F.col("overall") <= 60, "Ruim")
         .when(F.col("overall") <= 70, "Ok")
         .when(F.col("overall") <= 80, "Bom")
         .when(F.col("overall") <= 90, "Ótimo")
         .otherwise("Lenda")
    )
)

fut_players_classified.groupBy("classification").count().orderBy(
    "count", ascending=False
).display()

# Célula 4
# Seleciona jogadores brasileiros e os agrupa por posição
fut_brasil = fut_players.filter(F.col("nationality") == "Brazil")

fut_brasil = fut_brasil.withColumn(
    "position_group",
    F.when(F.col("position") == "GK", "Goleiro")
     .when(F.col("position").isin("CB", "LB", "RB", "LWB", "RWB"), "Defesa")
     .when(F.col("position").isin("CM", "CDM", "CAM", "LM", "RM"), "Meio")
     .when(F.col("position").isin("ST", "CF", "LW", "RW", "LF", "RF"), "Ataque")
     .otherwise("Outros")
)

# Seleciona os melhores jogadores de cada grupo para formar o 4-4-2
goleiro = (
    fut_brasil
    .filter(F.col("position_group") == "Goleiro")
    .orderBy(F.col("overall").desc())
    .limit(1)
)

defesa = (
    fut_brasil
    .filter(F.col("position_group") == "Defesa")
    .orderBy(F.col("overall").desc())
    .limit(4)
)

meio = (
    fut_brasil
    .filter(F.col("position_group") == "Meio")
    .orderBy(F.col("overall").desc())
    .limit(4)
)

ataque = (
    fut_brasil
    .filter(F.col("position_group") == "Ataque")
    .orderBy(F.col("overall").desc())
    .limit(2)
)

fut_players_sonho = (
    goleiro
    .union(defesa)
    .union(meio)
    .union(ataque)
)

fut_players_sonho = fut_players_sonho.select(
    "nationality",
    "position_group",
    "player_name",
    "overall"
)
display(fut_players_sonho)

# Célula 5
from pyspark.sql.window import Window

# Para cada jogador, identifica a versão com maior overall
window = Window.partitionBy("player_name").orderBy(
    F.col("overall").desc()
)

# Mantém apenas a melhor versão de cada jogador
fut_brasil_unico = (
    fut_brasil
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# 1 goleiro
goleiro = (
    fut_brasil_unico
    .filter(F.col("position_group") == "Goleiro")
    .orderBy(F.col("overall").desc())
    .limit(1)
)

# 4 defensores
defesa = (
    fut_brasil_unico
    .filter(F.col("position_group") == "Defesa")
    .orderBy(F.col("overall").desc())
    .limit(4)
)

# 4 meio-campistas
meio = (
    fut_brasil_unico
    .filter(F.col("position_group") == "Meio")
    .orderBy(F.col("overall").desc())
    .limit(4)
)

# 2 atacantes
ataque = (
    fut_brasil_unico
    .filter(F.col("position_group") == "Ataque")
    .orderBy(F.col("overall").desc())
    .limit(2)
)

# Montar o Dream Team
fut_players_sonho_bonus = (
    goleiro
    .union(defesa)
    .union(meio)
    .union(ataque)
    .select(
        "nationality",
        "position_group",
        "player_name",
        "overall"
    )
)

display(fut_players_sonho_bonus)