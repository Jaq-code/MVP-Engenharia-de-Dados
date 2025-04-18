--extração de dados paciente 
select top 3805 Sis_Pessoa.IDPessoa, CodPessoa, CodPessoa+' paciente' as Pessoa, Endereco, complemento, pais, cep, nascimento, sexo, EstadoCivil, Nacionalidade, Identidade,cpf,
STRING_AGG(Telefone, ' / ') AS Telefones
from Sis_Pessoa
join Sis_PessoaTelefone on Sis_PessoaTelefone.IDPessoa = sis_pessoa.IDPessoa
where IDPessoaTipo = 6
and Desativado = 0 
and hidden = 0 
and System = 0
group by Sis_Pessoa.IDPessoa, CodPessoa, Pessoa, Endereco, complemento, pais, cep, nascimento, sexo, EstadoCivil, Nacionalidade, Identidade,cpf

--extração de dados marcação 
select top 30000 IDMarcacao, PrimeiraConsulta, IsRevisao, IDAtendimento, IDMedico, IDEspecialidade, IDConvenio, DataMarcada, DataCancelamento, IDUsuarioInclusao, IsEmailConfirmado as Confirmado
from Age_Marcacao
where exists (
	select top 3805 *
	from Sis_Pessoa
	where IDPessoaTipo = 6
	and Desativado = 0 
	and hidden = 0 
	and System = 0
	and idpessoa = IDPaciente)
and DataMarcada < GETDATE()
order by IDMarcacao desc



--extração de dados especialidade
select IDEspecialidade, Especialidade
from Sis_Especialidade

--extração medicos
select vwSis_Medico.IDMEDICO, CodMedico, MEDICO, CRM,IDEspecialidade
from vwSis_Medico
join Sis_MedicoEspecialidade on Sis_MedicoEspecialidade.IDMedico = vwSis_Medico.IDMedico
where exists (select top 30000*
from Age_Marcacao
where exists (
	select top 3805 *
	from Sis_Pessoa
	where IDPessoaTipo = 6
	and Desativado = 0 
	and hidden = 0 
	and System = 0
	and idpessoa = IDPaciente)
and DataMarcada < GETDATE())
