
from mysql.connector import (connection)
import mariadb
from datetime import datetime
# from dateutil.parser import parse #pip install python-dateutil

import json

class Loader:

    def __init__(self, filename, cheptel):
        self.sql = mariadb.connect(user='root', password='xavier', host='127.0.0.1',
                                         database='gismodb')
        self.filename = filename
        self.cheptel = cheptel
        self.betesInBd = None
        self.beteMap = {}
        self.agnelageMap = {}
        self.agneauMap = {}
        self.sortieMap ={}

    def load(self):
        # Opening JSON file
        f = open(self.filename, )
        # returns JSON object as a dictionary
        base = json.load(f)
        # Closing file
        f.close()
        # base = slurper.parse(new  File(this.filename))
        for bete  in base["betes"] :
            idBd = self.present(bete)
            if idBd < 0:
                self.insertBete(bete)
            else:
                bete["idBd"] = idBd
                self.beteMap[bete["id"]] = bete
        for agnelage in base["agnelages"]:
            self.insertAgnelage(agnelage)
        for agneau in base["agneaux"]:
            self.insertAgneau(agneau)
        for echo in base["Echo"]:
            self.insertEcho(echo)
        for pesee in base["pesee"]:
            self.insertPesee(pesee) 
        for traitement in base["traitement"]:
            self.insertTraitement(traitement)
        for memo in base["memo"]:
            self.insertMemo(memo)

    def insertBete(self, bete):
        idBd = self.nextId()
        if bete["sex"] == "femelle":
            sex = 1
        else:
            sex = 0
        dateEntree = self.transformDate(bete["dateEntree"])
        dateSortie = self.transformDate(bete["dateSortie"])
        dateNaissance = self.transformDate(bete["dateNaissance"])
        motifEntree = self.motifEntree(bete["motifEntree"])
        motifSortie = self.motifSortie(bete["motifSortie"])
        if bete["nom"] == None :
            nom = "null"
        else:
            nom = bete["nom"].encode('unicode_escape')
        if bete["observations"] == None:
            obs =   "null"
        else :
            obs= bete["observations"].encode('unicode_escape')
        cursor = self.sql.cursor()
        sql = "INSERT INTO bete (" \
            "id,cheptel, dateEntree, dateNaissance, numBoucle, numMarquage, sex, dateSortie, motifEntree, motifSortie, nom, observations)" \
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (idBd, self.cheptel, dateEntree, dateNaissance,bete["numBoucle"], bete["numMarquage"], sex, dateSortie, motifEntree, motifSortie, nom, obs )
        cursor.execute(sql, val)
        bete["idBd"] = idBd
        self.beteMap[bete["id"]] = bete;

    def present(self, bete):
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

    def insertAgneau(self, agneau):
        if self.agnelageMap[agneau["agnelage_id"]]== None:
            return
        idBd = self.nextId()
        if (agneau["sex"] == "femelle"):
            sex = 1
        else:
            sex=0
        agnelage_id = self.agnelageMap[agneau["agnelage_id"]]["idBd"]
        allaitement = self.getAllaitement(agneau["allaitement"])
        sante =self.getSante(agneau["sante"])
        devenir_id = None
        if not agneau["devenir_id"] == None :
            devenir_id = self.beteMap[agneau["devenir_id"]]["idBd"]
        cursor = self.sql.cursor()

        cursor.execute("insert into agneaux " +
                    "(id, marquageProvisoire, sex, agnelage_id, devenir_id, allaitement, sante ) VALUES ("
                       "%s, %s, %s, %s, %s, %s, %s)",
                       (idBd, agneau["marquageProvisoire"], sex, agnelage_id, devenir_id, allaitement, sante ))
        agneau["idBd"] = idBd
        self.agneauMap[agneau["id"]] = agneau;
        cursor.close()

    def insertAgnelage(self, agnelage):
        if self.beteMap[agnelage["mere_id"] ] == None:
            return;
        idBd = self.nextId()
        dateAgnelage = self.transformDate(agnelage["dateAgnelage"]);
        mere_id = self.beteMap[agnelage["mere_id"]]["idBd"]
        cursor = self.sql.cursor()
        cursor.execute("insert into agnelage (" 
                    "id, dateAgnelage, mere_id, adoption, qualite, observations) VALUES " 
                    "(%s, %s, %s, %s, %s, %s)",
                    (idBd, dateAgnelage, mere_id, agnelage["adoption"], agnelage["qualite"], agnelage["observations"]))
        cursor.close()
        agnelage["idBd"] = idBd
        self.agnelageMap[agnelage["id"]] = agnelage

    def nextId(self):
        idBd = 0;
        cursor = self.sql.cursor(buffered=True)
        query = "select next_val as id_val from hibernate_sequence for update"
        cursor.execute(query)
        row = cursor.fetchone()
        idBd = row[0];
        cursor.close()
        cursor = self.sql.cursor(buffered=True)
        query = "update hibernate_sequence set next_val= %s where next_val=%s"
        val = (idBd + 1, idBd)
        cursor.execute(query, val)
        cursor.close()
        return idBd

    def insertEcho(self, echo):
        idBd = self.nextId()
        dateAgnelage = self.transformDate(echo ["dateAgnelage"])
        dateEcho = self.transformDate(echo["dateEcho"])
        dateSaillie = self.transformDate(echo["dateSaillie"])
        bete_id = self.beteMap[echo["bete_id"]]["idBd"]
        cursor = self.sql.cursor()
        cursor.execute("insert into echo (id, dateAgnelage, dateEcho, dateSaillie, nombre, bete_id) " 
                       "VALUES (%s, %s, %s, %s, %s, %s)",
                    (idBd, dateAgnelage, dateEcho, dateSaillie,echo["nombre"],bete_id))
        cursor.close()

    def insertPesee(self, pesee):
        idBd = self.nextId()
        datePesee = self.transformDate(pesee["datePesee"])
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

    def insertTraitement(self, traitement):
        idBd = self.nextId()
        debut  = self.transformDate( traitement["debut"] )
        fin = self.transformDate(traitement["fin"])
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
                "idBd, debut, fin, intervenant, medicament, motif, observation, bete_id, lamb_id, dose,duree, ordonnance, rythme,voie)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s)",
                ( idBd, debut, fin, traitement["intervenant"], traitement["medicament"], traitement["motif"], traitement["observation"], bete_id, lamb_id,
                  traitement["dose"], traitement["duree"], traitement["ordonnance"], traitement["rythme"], traitement["voie"]))
        cursor.close()

    def insertMemo(self, memo):
        idBd = self.nextId()
        debut = self.transformDate(memo["debut"]) #: "02/10/2022",
        fin = self.transformDate(memo["fin"]) #: "08/10/2022",
        #memo["classe"] #: "INFO",
        memo["note"] #: "diarrhée ",
        bete_id = self.beteMap[memo["bete_id"]]["idBd"] #: 49}
        cursor = self.sql.cursor()
        cursor.execute("insert into Note (id, debut, fin, note, bete_id) "
                       "VALUES (%s, %s, %s, %s, %s)",
                    (idBd, debut, fin, memo["note"], bete_id))
        cursor.close()

    def transformDate(self, date):
        if date is None:
            return None
        if date == "null":
            return None
        dateSQL = datetime.strptime(date, "%d/%m/%Y")
        return datetime.strftime(dateSQL, "%Y-%m-%d")

    def motifEntree(self, motif):
        motifValue = None
        if motif ==  "NAISSANCE":
            motifValue = 0
        elif  motif == "ACHAT":
            motifValue = 3
        return motifValue


    def motifSortie(self, motif):
        motifValue = None
        if motif == "MORT":
            motifValue = "1"
        elif motif == "VENTE_REPRODUCTEUR":
            motifValue = "3"
        return motifValue

    def getAllaitement(self, allaitement) :
        allaitId = -1
        if allaitement == "BIBERONNE":
            allaitId = 3
        elif allaitement == "ADOPTE":
            allaitId = 2
        elif allaitement == "ALLAITEMENT_MATERNEL":
            allaitId = 0
        return allaitId

    def getSante(self, sante):
        santeId = -1
        if sante == "VIVANT":
            santeId = 0
        if sante == "MORT_NE":
            santeId = 1
        if sante== "AVORTE":
            santeId = 2
        return santeId


