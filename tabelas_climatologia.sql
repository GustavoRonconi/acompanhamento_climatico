-- TABELAS DE CLIMATOLOGIA -- 




CREATE TABLE historico_horario
( id_historico_horario INTEGER PRIMARY KEY AUTOINCREMENT,
  cidade VARCHAR NOT NULL,
  uf VARCHAR NOT NULL,
  temperatura REAL,
  hora_previsao VARCHAR,
  descricao_clima VARCHAR,
  umidade REAL,
  chuva REAL,
  id_climatologia INTEGER,
  CONSTRAINT fk_climatologia
    FOREIGN KEY (id_climatologia)
    REFERENCES climatologia_atual(id_climatologia)
);




CREATE TABLE historico_diario
( id_historico_diario INTEGER PRIMARY KEY AUTOINCREMENT,
  cidade VARCHAR NOT NULL,
  uf VARCHAR NOT NULL,
  nascer_do_sol VARCHAR,
  por_do_sol VARCHAR,
  temperatura REAL,
  dia_previsao VARCHAR,
  descricao_clima VARCHAR,
  umidade REAL,
  chuva REAL,
  id_climatologia INTEGER,
  CONSTRAINT fk_climatologia
    FOREIGN KEY (id_climatologia)
    REFERENCES climatologia_atual(id_climatologia)
);