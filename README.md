# BCC362TP2-python

Esse repositório contém arquivos para colocar em prática os conceitos que estudamos na disciplina BCC362 (Sistemas Distribuídos) na UFOP.
Trata-se de simular o funcionamento de broker com clientes pub/sub.

Versão 1.0.0
- 1 broker, vários clientes (cada arquivo de cliente lança 3 threads simulando 3 clientes)
- como executar:
  - no diretório /src
  - na máquina escolhida para o broker:
      python3 broker.py <ip do broker> <porta do broker>
  - na máquina escolhida para o cliente:
      python3 client.py <ip do broker> <porta do broker> <ip do cliente> <porta do cliente> <nome do cliente>


Versão 2.0.0
- 1 broker principal, 1 broker backup, 2 variáveis e vários clientes (cada arquivo de cliente lança 3 threads simulando 3 clientes)
- como executar:
  - no diretório /src
  - na máquina escolhida para o broker principal:
      python3 broker_MAIN_2Var.py <ip do broker principal> <porta do broker pricipal> <ip do broker backup> <porta do broker backup>
  - na máquina escolhida para o broker backup:
      python3 tmp/broker_BACKUP_2Var.py <ip do broker backup> <porta do broker backup> <ip do broker principal> <porta do broker pricipal>
  - na máquina escolhida para um dos clientes:
      python3 client2var.py <nome do cliente> <ip do cliente> <ip do broker principal> <porta do broker pricipal> <ip do broker backup> <porta do broker backup>
  - na máquina escolhida para o outro cliente:
      python3 tmp/client_2var.py <nome do cliente> <ip do cliente> <ip do broker principal> <porta do broker pricipal> <ip do broker backup> <porta do broker backup>
