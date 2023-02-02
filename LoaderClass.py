# from mysql.connector import (connection)
# import mariadb
import mysql.connector
from datetime import datetime
# from dateutil.parser import parse #pip install python-dateutil

import json


class Loader:

    def __init__(self, filename, cheptel):
        self.sql = mysql.connector.connect(user='root', password='xavier', host='127.0.0.1',
                                           database='gismodb')
        self.filename = filename
        self.cheptel = cheptel
        self.betesInBd = None
        self.beteMap = {}
        self.agnelageMap = {}
        self.agneauMap = {}
        self.sortieMap = []

    def load(self):
        # Opening JSON file
        f = open(self.filename, )
        # returns JSON object as a dictionary
        base = json.load(f)
        # Closing file
        f.close()
        # base = slurper.parse(new  File(this.filename))
        for bete in base["betes"]:
            idBd = self.__present(bete)
            if idBd < 0:
                self.__insertBete(bete)
            else:
                bete["idBd"] = idBd
                self.beteMap[bete["id"]] = bete
            self.__manageSortie(bete)

        for agnelage in base["agnelages"]:
            self.__insertAgnelage(agnelage)
        for agneau in base["agneaux"]:
            self.__insertAgneau(agneau)
        for echo in base["Echo"]:
            self.__insertEcho(echo)
        for pesee in base["pesee"]:
            self.__insertPesee(pesee)
        for traitement in base["traitement"]:
            self.__insertTraitement(traitement)
        for memo in base["memo"]:
            self.__insertMemo(memo)


    def __insertBete(self, bete):
        idBd = self.__nextId()
        if bete["sex"] == "femelle":
            sex = 1
        else:
            sex = 0
        dateEntree = self.__transformDate(bete["dateEntree"])
        dateSortie = self.__transformDate(bete["dateSortie"])
        dateNaissance = self.__transformDate(bete["dateNaissance"])
        motifEntree = self.__motifEntree(bete["motifEntree"])
        motifSortie = self.__motifSortie(bete["motifSortie"])
        if bete["nom"] is None:
            nom = "null"
        else:
            nom = bete["nom"].encode('unicode_escape')
        if bete["observations"] is None:
            obs = "null"
        else:
            obs = bete["observations"].encode('unicode_escape')
        cursor = self.sql.cursor()
        sql = "INSERT INTO bete (" \
              "id,cheptel, dateEntree, dateNaissance, numBoucle, numMarquage, " \
              "sex, dateSortie, motifEntree, motifSortie, nom, observations)" \
              " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (idBd, self.cheptel, dateEntree, dateNaissance, bete["numBoucle"], bete["numMarquage"], sex, dateSortie,
               motifEntree, motifSortie, nom, obs)
        cursor.execute(sql, val)
        cursor.close()
        bete["idBd"] = idBd
        self.beteMap[bete["id"]] = bete

    def __manageSortie(self, bete):
        if bete["dateSortie"] is None:
            return
        sortie = self.__searchSortie(bete)
        if sortie is None:
            sortie = self.__insertSortie(bete)
        cursor = self.sql.cursor()
        sql = "UPDATE bete set sortie_id = %s WHERE id=%s"
        val = (sortie["id"], bete["idBd"])
        cursor.execute(sql, val)
        cursor.close()

    def __present(self, bete):
        cursor = self.sql.cursor(dictionary=True, buffered=True)
        query = ("select * from bete where numBoucle= %s"
                 " AND numMarquage= %s"
                 " AND cheptel= %s")
        cursor.execute(query, (bete["numBoucle"], bete["numMarquage"], self.cheptel))
        myresult = cursor.fetchone()
        if myresult is not None:
            return myresult["id"]
        cursor.close()
        return -1

    def __insertAgneau(self, agneau):
        if self.agnelageMap[agneau["agnelage_id"]] is None:
            return
        idBd = self.__nextId()
        if agneau["sex"] == "femelle":
            sex = 1
        else:
            sex = 0
        agnelage_id = self.agnelageMap[agneau["agnelage_id"]]["idBd"]
        allaitement = self.__getAllaitement(agneau["allaitement"])
        sante = self.__getSante(agneau["sante"])
        devenir_id = None
        if not agneau["devenir_id"] is None:
            devenir_id = self.beteMap[agneau["devenir_id"]]["idBd"]
        cursor = self.sql.cursor()

        cursor.execute("insert into agneaux " +
                       "(id, marquageProvisoire, sex, agnelage_id, devenir_id, allaitement, sante ) VALUES ("
                       "%s, %s, %s, %s, %s, %s, %s)",
                       (idBd, agneau["marquageProvisoire"], sex, agnelage_id, devenir_id, allaitement, sante))
        agneau["idBd"] = idBd
        self.agneauMap[agneau["id"]] = agneau
        cursor.close()

    def __insertAgnelage(self, agnelage):
        if self.beteMap[agnelage["mere_id"]] is None:
            return
        idBd = self.__nextId()
        dateAgnelage = self.__transformDate(agnelage["dateAgnelage"])
        mere_id = self.beteMap[agnelage["mere_id"]]["idBd"]
        cursor = self.sql.cursor()
        cursor.execute("insert into agnelage ("
                       "id, dateAgnelage, mere_id, adoption, qualite, observations) VALUES "
                       "(%s, %s, %s, %s, %s, %s)",
                       (idBd, dateAgnelage, mere_id, agnelage["adoption"], agnelage["qualite"],
                        agnelage["observations"]))
        cursor.close()
        agnelage["idBd"] = idBd
        self.agnelageMap[agnelage["id"]] = agnelage

    def __nextId(self):
        cursor = self.sql.cursor(buffered=True)
        query = "select next_val as id_val from hibernate_sequence for update"
        cursor.execute(query)
        row = cursor.fetchone()
        idBd = row[0]
        cursor.close()
        cursor = self.sql.cursor(buffered=True)
        query = "update hibernate_sequence set next_val= %s where next_val=%s"
        val = (idBd + 1, idBd)
        cursor.execute(query, val)
        cursor.close()
        return idBd

    def __insertEcho(self, echo):
        idBd = self.__nextId()
        dateAgnelage = self.__transformDate(echo["dateAgnelage"])
        dateEcho = self.__transformDate(echo["dateEcho"])
        dateSaillie = self.__transformDate(echo["dateSaillie"])
        bete_id = self.beteMap[echo["bete_id"]]["idBd"]
        cursor = self.sql.cursor()
        cursor.execute("insert into Echo (id, dateAgnelage, dateEcho, dateSaillie, nombre, bete_id) "
                       "VALUES (%s, %s, %s, %s, %s, %s)",
                       (idBd, dateAgnelage, dateEcho, dateSaillie, echo["nombre"], bete_id))
        cursor.close()

    def __insertPesee(self, pesee):
        idBd = self.__nextId()
        datePesee = self.__transformDate(pesee["datePesee"])
        poids = pesee["poids"]
        if pesee["bete_id"] is not None:
            bete_id = self.beteMap[pesee["bete_id"]]["idBd"]
        else:
            bete_id = None
        if pesee["lamb_id"] is not None:
            lamb_id = self.agneauMap[pesee["lamb_id"]]["idBd"]
        else:
            lamb_id = None
        cursor = self.sql.cursor()
        cursor.execute("insert into pesee (id, datePesee, poids, bete_id, lamb_id) "
                       "VALUES (%s, %s, %s, %s, %s)",
                       (idBd, datePesee, poids, bete_id, lamb_id))
        cursor.close()

    def __insertTraitement(self, traitement):
        idBd = self.__nextId()
        debut = self.__transformDate(traitement["debut"])
        fin = self.__transformDate(traitement["fin"])
        if traitement["beteId"] is not None:
            bete_id = self.beteMap[traitement["beteId"]]["idBd"]
        else:
            bete_id = None
        if traitement["lambId"] is not None:
            lamb_id = self.agneauMap[traitement["lambId"]]["idBd"]
        else:
            lamb_id = None
        cursor = self.sql.cursor()
        cursor.execute("insert into traitement ("
                       "idBd, debut, fin, intervenant, medicament, motif, observation, "
                       "bete_id, lamb_id, dose,duree, ordonnance, rythme,voie)"
                       " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s)",
                       (idBd, debut, fin, traitement["intervenant"], traitement["medicament"], traitement["motif"],
                        traitement["observation"], bete_id, lamb_id,
                        traitement["dose"], traitement["duree"], traitement["ordonnance"], traitement["rythme"],
                        traitement["voie"]))
        cursor.close()

    def __insertMemo(self, memo):
        idBd = self.__nextId()
        debut = self.__transformDate(memo["debut"])
        fin = self.__transformDate(memo["fin"])
        bete_id = self.beteMap[memo["bete_id"]]["idBd"]
        cursor = self.sql.cursor()
        cursor.execute("insert into Note (id, debut, fin, note, bete_id) "
                       "VALUES (%s, %s, %s, %s, %s)",
                       (idBd, debut, fin, memo["note"], bete_id))
        cursor.close()

    @staticmethod
    def __transformDate(date):
        if date is None:
            return None
        if date == "null":
            return None
        dateSQL = datetime.strptime(date, "%d/%m/%Y")
        return datetime.strftime(dateSQL, "%Y-%m-%d")

    @staticmethod
    def __motifEntree(motif):
        motifValue = None
        if motif == "NAISSANCE":
            motifValue = 0
        elif motif == "ACHAT":
            motifValue = 3
        elif motif == 'CREATION':
            motifValue = 1
        return motifValue

    @staticmethod
    def __motifSortie(motif):
        motifValue = None
        if motif == "MORT":
            motifValue = "1"
        elif motif == "VENTE_REPRODUCTEUR":
            motifValue = "3"
        elif motif == "AUTO_CONSOMMATION":
            motifValue = "8"
        return motifValue

    @staticmethod
    def __getAllaitement(allaitement):
        allaitId = -1
        if allaitement == "BIBERONNE":
            allaitId = 3
        elif allaitement == "ADOPTE":
            allaitId = 2
        elif allaitement == "ALLAITEMENT_MATERNEL":
            allaitId = 0
        return allaitId

    @staticmethod
    def __getSante(sante):
        santeId = -1
        if sante == "VIVANT":
            santeId = 0
        elif sante == "MORT_NE":
            santeId = 1
        elif sante == "AVORTE":
            santeId = 2
        return santeId

    def __searchSortie(self, bete):
        for sortie in self.sortieMap:
            if sortie["dateSortie"] == bete["dateSortie"] and sortie["cause"] == bete["motifSortie"]:
                return sortie
        return None

    def __insertSortie(self, bete):
        cause = self.__getCauseSortie(bete["motifSortie"])
        dateSortie = self.__transformDate(bete["dateSortie"])
        idBd = self.__nextId()
        cursor = self.sql.cursor()
        cursor.execute("insert into Sortie (id, cause, dateSortie, cheptel) "
                       "VALUES (%s, %s, %s, %s)",
                       (idBd, cause, dateSortie, self.cheptel ))
        cursor.close()
        sortie = {"id": idBd, "cause": bete["motifSortie"], "dateSortie": bete["dateSortie"]}
        self.sortieMap.append(sortie)
        return sortie

    @staticmethod
    def __getCauseSortie(cause):
        if cause == "VENTE_REPRODUCTEUR":
            return 3
        elif cause == "MORT":
            return 1
        elif cause == "VENTE_BOUCHERIE":
            return 2
        elif cause == "AUTO_CONSOMMATION":
            return 8
        elif cause == "INCONNUE":
            return 9
