package com.ht.hl7fusion;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/****************************************************************
 * Générateur de fichier de fusion d'identité
 * compatible avec la GAM Maincare 
 * Ce système se base sur une extraction de la base de donnée
 * maincare PA_PAT
 * select to_char (etc_per_deb,'YYYYMMDDHH24MISS')  as "DHFUS",
 * pat_ipp as "esclave",
 * pat_imr as "maitre",
 * etc_nom,
 * ETC_NOM_MAR,
 * etc_prn,
 * to_char(ETC_DDN,'YYYYMMDD') || '000000' as "DDN", 
 * ETC_SEX, 
 * ETC_TIT, 
 * ADR_NUM_TEL_001,
 * ADR_EMAIL_001, 
 * TRIM(SUBSTR(ADR_ADR_001_CON,0,70)), 
 * ADR_ADR_001_COD_POS, 
 * ADR_ADR_001_NOM_VIL 
 * from pa_pat where pat_imr is not null and ETC_PER_DEB<TO_DATE('01/07/2020','DD/MM/YYYY') order by etc_per_deb;
 * 
 * @author tondeur-h
 ****************************************************************/
public class HL7Fusion {
    
    String DDJ;
    String NUMMSG;
    String IPPMAITRE;
    String NOMUSG;
    String PRN;
    String TIT;
    String NOMNAIS;
    String SEX;
    String ADR;
    String VILLE;
    String DDN;
    String CP;
    String TELMAIL;
    String IPPESC;
    String MAIL;
    String TEL;
    
    //messages spécifique pour la GAM MainCare
    //message avec nom usage + nom naissance
    String msgMultiOut="MSH|^~\\&|SA_AMCK|SF_MCK|DXSRA|Resultat labo|#DDJ#|F01|ADT^A40|#NUMMSG#|P|2.3.1||||||8859/1|\r" +
                       "EVN|A40|#DDJ#||||\r" +
                       "PID|1||#IPPMAITRE#^^^IF_MCKN||#NOMNAIS#^#PRN#^^^#TIT#^^L~#NOMUSG#^#PRN#^^^^^M|#NOMNAIS#|#DDN#|#SEX#|#NOMNAIS#||#ADR#^^#VILLE#^^#CP#^100|100|#TELMAIL#|||U||||||||||||100^100||N\r" +
                       "MRG|#IPPESC#^^^IF_MCKN|||#IPPESC#^^^IF_MCKN|||";
    //message avec nom naissance uniquement
    String msgMonoOut="MSH|^~\\&|SA_AMCK|SF_MCK|Doctolib|Doctolib|#DDJ#|F01|ADT^A40|#NUMMSG#|P|2.3.1||||||8859/1|\r" +
                      "EVN|A40|#DDJ#||||\r" +
                      "PID|1||#IPPMAITRE#^^^IF_MCKN||#NOMNAIS#^#PRN#^^^#TIT#^^L||#DDN#|#SEX#|||#ADR#^^#VILLE#^^#CP#^100|100|#TELMAIL#|||U||||||||||||100^100||N\r" +
                      "MRG|#IPPESC#^^^IF_MCKN|||#IPPESC#^^^IF_MCKN|||";
    
    String fileName="C:/dev/HL7Fusion/patients.csv";
    String basePath="c:/dev/HL7Fusion/hl7/";
    String extensionHL7=".hl7";
    
    /**********************************************************************************************
     * Le fichier CSV doit être séparé par des ;
     * comporter 14 champs dans l'ordre suivant
     *          IPPESC = IPP a fusionner (doit disparaitre)
                IPPMAITRE = IPP a conserver
                NOMNAIS= Nom de naissance du patient (obligatoire)    
                NOMUSG= Nom d'usage du patient (optionnel)
                PRN= Prenom du patient (Obligatoire)
                DDN Date de naissance du patient au format YYYYMMDDHHMISS (Obligatoire)
                SEX= Sexe du patient M ou F (Obligatoire)
                TIT= Titre du patient M. Mme Mle etc... (Optionnel)
                TEL Num tel patient sur 10 digits (optionnel)
                MAIL= mail du patient (optionnel)
                ADR= Adresse du patient sans le code postal, ni la ville (Une seule ligne)
                CP= Code postal de résidence du patient
                VILLE= Ville de résidence du patient
     *  Exemple:
     * 123456;456789;TEST;;HERVE;19340112000000;M;M.;0601020304;herve.t@monmail.fr;11 rue du jour;59300;Valenciennes
     ***********************************************************************************************/
    
    
    /******************************
     * 
     * @param argv 
     ******************************/
    public static void main(String[] argv)
    {
        new HL7Fusion();
    }

    /******************************
     * Constructeur 
     ******************************/
    public HL7Fusion() 
    {
        //lister chaque lignes
        Path myPath = Paths.get(fileName);
        CSVParser parser = new CSVParserBuilder().withSeparator(';').build();

        try (var br = Files.newBufferedReader(myPath,  StandardCharsets.UTF_8);
             var reader = new CSVReaderBuilder(br).withCSVParser(parser)
                     .build()) {
            List<String[]> rows = reader.readAll();
            rows.stream().map(row -> {    
                getData(row);
                return row;
            }).map(_item -> {
                Nettoyer_les_donnees();
                return _item;
            }).forEachOrdered(_item -> {
                Construire_fichier(basePath+NUMMSG+IPPESC+extensionHL7);
            });
        } catch (IOException | CsvException ex) {
            Logger.getLogger(HL7Fusion.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /********************************
     * Get Data from CSV File
     * @param row 
     ********************************/
    private void getData(String[] row)
    {
                DDJ=calculate_DDJ();
                NUMMSG=Long.toString(System.currentTimeMillis(),10);
                
                IPPESC=row[1];
                IPPMAITRE=row[2];
                NOMNAIS=row[3];                
                NOMUSG=row[4];
                PRN=row[5];
                DDN=row[6];
                SEX=row[7];                
                TIT=row[8];
                TEL=row[9];
                MAIL=row[10];
                ADR=row[11];
                CP=row[12];
                VILLE=row[13];
    }
    
    /*******************************
     * Construire fichier HL7
     *******************************/
    private void Construire_fichier(String nom_fichier) 
    {
        try {
            FileWriter fw;
            //construite TELMAIL
            if (MAIL.length()<6) {TELMAIL=TEL;} else {TELMAIL=TEL+"^^^"+MAIL;}
            
            //Choisir le message et appliquer les régles identités
            String MSG="";
            //si nom de naissance est vide et nom usage non vide alors
            // nom naissance = nom usage et nomusage a vider
            if (NOMNAIS.compareToIgnoreCase("")==0 && NOMUSG.compareToIgnoreCase("")!=0) {NOMNAIS=NOMUSG;NOMUSG="";}
            if (NOMNAIS.compareToIgnoreCase("")!=0 && NOMUSG.compareToIgnoreCase("")!=0) {MSG=msgMultiOut;} else {MSG=msgMonoOut;}
            
            //remplacer
            String HL7_OUT=replace_all(MSG);
            
            //ecrire les fichiers
            fw = new FileWriter(nom_fichier);
            fw.write(HL7_OUT);
            fw.close();
        } catch (IOException ex) {
            Logger.getLogger(HL7Fusion.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
      
    /********************************
     * Remplacer toutes les variables
     * @param MSG
     * @return 
     *********************************/
        private String replace_all(String MSG) 
    {
        String retour=MSG;
        //proceder aux remplacements dans la chaine de caractéres 
        retour=retour.replaceAll("#DDJ#", DDJ);
        retour=retour.replaceAll("#NUMMSG#", NUMMSG);
        retour=retour.replaceAll("#IPPMAITRE#", IPPMAITRE);
        retour=retour.replaceAll("#NOMUSG#", NOMUSG);
        retour=retour.replaceAll("#PRN#", PRN);
        retour=retour.replaceAll("#TIT#", TIT);
        retour=retour.replaceAll("#NOMNAIS#", NOMNAIS);
        retour=retour.replaceAll("#SEX#", SEX);
        retour=retour.replaceAll("#DDN#", DDN);
        retour=retour.replaceAll("#ADR#", ADR);
        retour=retour.replaceAll("#VILLE#", VILLE);
        retour=retour.replaceAll("#CP#", CP);
        retour=retour.replaceAll("#TELMAIL#", TELMAIL);
        retour=retour.replaceAll("#IPPESC#", IPPESC);
        return retour;
    }

    /*******************************
     * Nettoyer et étendre les données
     * *****************************/
    private void Nettoyer_les_donnees() {
        //nettoyer TEL et mettre sur 10 digits
        TEL=TEL.replaceAll("\\.", "");
        TEL=TEL.replaceAll("-", "");
        TEL=TEL.replaceAll("#", "");
        TEL=TEL.replaceAll(" ", "");
        TEL=TEL.replaceAll("/", "");
        try{TEL=TEL.substring(0, 10);} catch (Exception e) {TEL="";}
        try{Integer.parseInt(TEL, 10);} catch (NumberFormatException e) {TEL="";}
    }

    
    /*********************************
     * Date Time du Jour
     * @return 
     *********************************/
    private String calculate_DDJ()
    {
        String reponse;
        DateFormat formatDT = new SimpleDateFormat("yyyyMMddHHmmss");
        Calendar calendar = Calendar.getInstance();
        reponse=formatDT.format(calendar.getTime());  
        return reponse;
    }
    
}
