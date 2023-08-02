library(RPostgres)

drv<-dbDriver("PostgreSQL")

con<-dbConnect(drv,
               dbname="UfficioDellaMotorizzazione",
               host="localhost",
               port=5432,
               user="postgres",
               password="fl13018j")

#Popolamento tabella Info_privati: prendi 700 codici proprietari, e li associ ai privati
temp_cprop <- dbGetQuery(con, "select id_proprietario
                               from motorizzazione.proprietario")
temp_cprop <- temp_cprop$id_proprietario
info_privati.codice_proprietario<-sample(temp_cprop, 700, replace=F)

temp_cf<-dbGetQuery(con, "select cf
                          from motorizzazione.privato")

info_privati.privato<-temp_cf$cf

info_privati_df<-data.frame(privato=info_privati.privato,
                            codice_proprietario=info_privati.codice_proprietario)
dbWriteTable(con, name=c("motorizzazione","info_privati"),value=info_privati_df, append=T, row.names=F)


#popolamento tabella "Produttore"
v_nomi_produttori<-readLines("C:\\Users\\lollo\\Desktop\\BASI DI DATI\\PROGETTO\\nomi_produttori.txt")
temp_cprop <- dbGetQuery(con, "select id_proprietario
                               from motorizzazione.proprietario as p
                               where not exists(select *
                                                from motorizzazione.info_privati as ip
                                                where p.id_proprietario=ip.codice_proprietario)")
temp_cprop <- temp_cprop$id_proprietario
produttore.codice_proprietario<-sample(temp_cprop, 10, replace=F)
produttore_df<-data.frame(nome=v_nomi_produttori,
                            codice_proprietario=produttore.codice_proprietario)

dbWriteTable(con, name=c("motorizzazione","produttore"),value=produttore_df, append=T, row.names=F)


#popolamento tabella "Rivenditore"
v_nomi_rivenditori<-readLines("C:\\Users\\lollo\\Desktop\\BASI DI DATI\\PROGETTO\\nomi_rivenditori.txt")
temp_cprop <- dbGetQuery(con, "select id_proprietario
                               from motorizzazione.proprietario as p
                               where not exists(select *
                                                from motorizzazione.info_privati as ip
                                                where p.id_proprietario=ip.codice_proprietario)
                                     and not exists(select
                                                    from motorizzazione.produttore
                                                    where p.id_proprietario=codice_proprietario)")
temp_cprop <- temp_cprop$id_proprietario
rivenditore.codice_proprietario<-sample(temp_cprop, 180, replace=F)
rivenditore_df<-data.frame(nome=v_nomi_rivenditori,
                          codice_proprietario=rivenditore.codice_proprietario)

dbWriteTable(con, name=c("motorizzazione","rivenditore"),value=rivenditore_df, append=T, row.names=F)

#popolamento tabella "Auto"
temp_nuovo_proprietario<-temp_nuovo_proprietario$id_proprietario
auto.nuovo_proprietario<-sample(temp_nuovo_proprietario,1000,replace=T)
v_anno_di_produzione<-readLines("C:\\Users\\lollo\\Desktop\\BASI DI DATI\\PROGETTO\\anno_di_produzione.txt")
temp_mod<-dbGetQuery(con, "select nome from motorizzazione.modello_auto")
temp_mod<-temp_mod$nome
auto.nome_modello<-sample(temp_mod,1000,replace=T)
auto_df<-data.frame(numero_seriale=v_numeri_seriali,
                    nome_modello=auto.nome_modello,
                    anno_di_produzione=v_anno_di_produzione,
                    usata=FALSE)
dbWriteTable(con, name=c("motorizzazione","auto"),value=auto_df, append=T, row.names=F)


#popolamento tabella "Atto_di_trasferimento"

temp_auto_trasf<-dbGetQuery(con, "select numero_seriale as num_auto_trasferita, nome_modello as mod_auto_trasferita from motorizzazione.auto")

temp_nuovo_proprietario<-dbGetQuery(con, "select id_proprietario from motorizzazione.proprietario")
temp_nuovo_proprietario<-temp_nuovo_proprietario$id_proprietario
auto.nuovo_proprietario<-sample(temp_nuovo_proprietario,1000,replace=T)
temp_vecchio_proprietario<-dbGetQuery(con, "select id_proprietario from motorizzazione.proprietario")
temp_vecchio_proprietario<-temp_vecchio_proprietario$id_proprietario
auto.vecchio_proprietario<-sample(temp_vecchio_proprietario,1000,replace=T)
v_data_trasferimento<-readLines("C:\\Users\\lollo\\Desktop\\BASI DI DATI\\PROGETTO\\data_trasferimento.txt")
v_idatto<-readLines("C:\\Users\\lollo\\Desktop\\BASI DI DATI\\PROGETTO\\id_atto.txt")
atti_df<-data.frame(id_atto=v_idatto,
                    nuovo_proprietario=auto.nuovo_proprietario,
                    vecchio_proprietario=auto.vecchio_proprietario,
                    data_trasferimento=v_data_trasferimento)
atti_df<-data.frame(atti_df,temp_auto_trasf)
dbWriteTable(con, name=c("motorizzazione","atto_di_trasferimento"),value=atti_df, append=T, row.names=F)

