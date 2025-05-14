package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App2 {
    public static void main(String[] args) {
        // Configuration de Spark
        SparkConf conf = new SparkConf()
                .setAppName("Total Ventes Par Ville Et Année")
                .setMaster("local[*]"); // Mode local pour les tests

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Définition du chemin du fichier d'entrée
        String inputPath = "ventes.txt";

        // Lecture du fichier
        JavaRDD<String> ventesRDD = sc.textFile(inputPath);

        // Transformation des données
        // Structure: date ville produit prix
        JavaPairRDD<Tuple2<String, String>, Double> ventesParVilleEtAnnee = ventesRDD
                .mapToPair(ligne -> {
                    String[] champs = ligne.split(" ");
                    // Extraction de la date (champ 0), la ville (champ 1) et du prix (champ 3)
                    String date = champs[0];
                    // Extraction de l'année depuis la date (supposant un format comme "YYYY-MM-DD")
                    String annee = date.split("-")[0];
                    String ville = champs[1];
                    Double prix = Double.parseDouble(champs[3]);

                    // Clé composite (ville, année)
                    return new Tuple2<>(new Tuple2<>(ville, annee), prix);
                })
                .reduceByKey((prix1, prix2) -> prix1 + prix2); // Somme des prix par (ville, année)

        // Affichage des résultats
        System.out.println("Total des ventes par ville et par année:");
        ventesParVilleEtAnnee.collect().forEach(resultat -> {
            Tuple2<String, String> cle = resultat._1();
            String ville = cle._1();
            String annee = cle._2();
            Double totalVentes = resultat._2();
            System.out.println("Ville: " + ville + ", Année: " + annee + ", Total: " + totalVentes);
        });

        // Sauvegarde des résultats si nécessaire
        // ventesParVilleEtAnnee.saveAsTextFile("resultats_ventes_par_ville_et_annee");

        // Arrêt du contexte Spark
        sc.close();
    }
}
