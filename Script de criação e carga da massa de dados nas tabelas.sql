/********************************************************************************
  * -- Script de criação das tabelas transacionais e carga de dados nas tabelas 
  * 
 Data de Criação | Desenvolvedor 		| Versão
 07/09/2024      | Anderson Lima Rocha  | 1.0
 * ******************************************************************************/

-- Tabela de Condominios
CREATE TABLE condominios ( 
	condominio_id SERIAL PRIMARY KEY, 
	nome VARCHAR(255) NOT NULL, 
	endereco TEXT NOT NULL);

-- Tabela de moradores
CREATE TABLE moradores (
	morador_id SERIAL PRIMARY KEY,
	nome VARCHAR(255) NOT NULL, condominio_id INT NOT NULL,
	data_registro DATE NOT NULL,
	FOREIGN KEY (condominio_id) REFERENCES
	condominios(condominio_id) 
);

-- Tabela de imoveis
CREATE TABLE imoveis (
	imovel_id SERIAL PRIMARY KEY,
	tipo VARCHAR(50) NOT NULL,
	condominio_id INT NOT NULL,
	valor NUMERIC(15, 2) NOT NULL,
	FOREIGN KEY (condominio_id) REFERENCES
	condominios(condominio_id) 
);

-- Tabela de Transacoes 
CREATE TABLE transacoes (
	transacao_id SERIAL PRIMARY KEY,
	imovel_id INT NOT NULL,
	morador_id INT NOT NULL,
	data_transacao DATE NOT NULL,
	valor_transacao NUMERIC(15, 2) NOT NULL,
	FOREIGN KEY (imovel_id) REFERENCES imoveis(imovel_id), 
	FOREIGN KEY (morador_id) REFERENCES moradores(morador_id)
)
;

/*****************************************************************************************
		 ********** -- Carga de Dados Fakes nas tabelas transacionais --  **********
 *****************************************************************************************/

-- 1. Dados para a tabela condominios
INSERT INTO condominios (nome, endereco) VALUES
('Condomínio Jardins', 'Rua das Flores, 123'),
('Condomínio Primavera', 'Av. Central, 456'),
('Condomínio Residencial Sol', 'Rua do Sol, 789'),
('Condomínio Estrela', 'Av. Estrela, 321'),
('Condomínio Bela Vista', 'Rua Vista Alegre, 654');

-- 2. Dados para a tabela moradores
INSERT INTO moradores (nome, condominio_id, data_registro) VALUES
('João Silva', 1, '2022-01-15'),
('Maria Oliveira', 2, '2022-02-10'),
('Carlos Pereira', 3, '2022-03-05'),
('Ana Souza', 1, '2022-04-12'),
('Pedro Almeida', 4, '2022-05-20');

-- 3. Dados para a tabela imoveis
INSERT INTO imoveis (tipo, condominio_id, valor) VALUES
('Apartamento', 1, 500000.00),
('Casa', 2, 800000.00),
('Apartamento', 3, 450000.00),
('Casa', 4, 1200000.00),
('Apartamento', 5, 650000.00);

-- 4. Dados para a tabela transacoes
INSERT INTO transacoes (imovel_id, morador_id, data_transacao, valor_transacao) VALUES
(1, 1, '2023-01-20', 500000.00),
(2, 2, '2023-02-15', 800000.00),
(3, 3, '2023-03-10', 450000.00),
(4, 4, '2023-04-05', 1200000.00),
(5, 5, '2023-05-25', 650000.00);

