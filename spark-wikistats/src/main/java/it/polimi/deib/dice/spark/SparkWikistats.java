package it.polimi.deib.dice.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by miik on 15/09/17.
 */
public class SparkWikistats {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String args[]){

        Page page1 = new Page(
                "7697611",
                "Archer (typeface)",
                "<page>\n" +
                "    <title>Archer (typeface)</title>\n" +
                "    <ns>0</ns>\n" +
                "    <id>7697611</id>\n" +
                "    <revision>\n" +
                "      <id>797421154</id>\n" +
                "      <parentid>761722748</parentid>\n" +
                "      <timestamp>2017-08-26T23:32:36Z</timestamp>\n" +
                "      <contributor>\n" +
                "        <username>Bellerophon5685</username>\n" +
                "        <id>1258165</id>\n" +
                "      </contributor>\n" +
                "      <comment>/* External links */</comment>\n" +
                "      <model>wikitext</model>\n" +
                "      <format>text/x-wiki</format>\n" +
                "      <text xml:space=\"preserve\">{{Multiple issues|\n" +
                "{{original research|date=June 2010}}\n" +
                "{{notability|date=June 2010}}\n" +
                "}}\n" +
                "\n" +
                "{{Infobox typeface\n" +
                "| name = Archer\n" +
                "| image = ArcherSpec.svg\n" +
                "| style = [[Serif]]\n" +
                "| classifications = [[Slab serif|Humanist slab serif]]\n" +
                "| date = 2001\n" +
                "| creator = [[Tobias Frere-Jones]]&lt;br&gt;\n" +
                "[[Jonathan Hoefler]]\n" +
                "| foundry = [[Hoefler &amp; Frere-Jones]]\n" +
                "}}\n" +
                "\n" +
                "'''Archer''' is a [[slab serif]] typeface designed in 2001 by [[Tobias Frere-Jones]] and [[Jonathan Hoefler]] for use in ''[[Martha Stewart Living]]'' magazine. It was later released by [[Hoefler &amp; Frere-Jones]] for commercial licensing.\n" +
                "\n" +
                "==Structure==\n" +
                "\n" +
                "The typeface is a [[Slab_serif#Geometric_model|geometric]] slab serif, one with a geometric design similar to sans-serif fonts. It takes inspiration from mid-twentieth century designs such as [[Rockwell (typeface)|Rockwell]].\n" +
                "\n" +
                "The face is unique for combining the geometric structure of twentieth-century European slab-serifs but imbuing the face with a domestic, less strident tone of voice. [[Ball terminal|Balls]] were added to the upper terminals on letters such as ''C'' and ''G'' to increase its charm.&lt;ref&gt;{{cite web|last1=Devroye|first1=Luc|title=Jonathan Hoefler|url=http://luc.devroye.org/fonts-27370.html|publisher=McGill University|accessdate=29 September 2014}}&lt;/ref&gt; [[Italic type|Italics]] are true italic designs, with flourishes influenced by calligraphy, an unusual feature for geometric slab serif designs. As with many [[Hoefler &amp; Frere-Jones]] designs, it was released in a wide range of weights from hairline to bold, reflecting its design goal as a typeface for complex magazines.&lt;ref name=&quot;Typographica review&quot;&gt;{{cite web|last1=Earls|first1=David John|title=Archer|url=http://typographica.org/typeface-reviews/archer/|website=Typographica|accessdate=11 July 2015}}&lt;/ref&gt;\n" +
                "\n" +
                "==Uses==\n" +
                "The typeface has been used for, among other things, branding for [[Wells Fargo]] and is a main font for the ''[[San Francisco Chronicle]]'' and [[Wes Anderson]]'s film ''[[The Grand Budapest Hotel]]''.&lt;ref&gt;{{cite web|last1=Adams|first1=Lauren|title=Is Archer's Use on Target?|url=http://www.aiga.org/is-archers-use-on-target/|publisher=AIGI}}&lt;/ref&gt; It is also the current font used for titles and body text by the [[Design Observer]] website.\n" +
                "\n" +
                "==References==\n" +
                "{{Reflist}}\n" +
                "\n" +
                "==External links==\n" +
                "*[http://www.typography.com/fonts/font_overview.php?productLineID=100033 Archer] (H&amp;FJ website)\n" +
                "\n" +
                "[[Category:Hoefler &amp; Frere-Jones typefaces]]\n" +
                "[[Category:Typefaces designed by Tobias Frere-Jones]]\n" +
                "[[Category:Typefaces designed by Jonathan Hoefler]]\n" +
                "[[Category:Slab serif typefaces]]\n" +
                "[[Category:Typefaces and fonts introduced in 2001]]\n" +
                "[[Category:Digital typefaces]]\n" +
                "[[Category:Typefaces with text figures]]\n" +
                "[[Category:Geometric slab-serif typefaces]]\n" +
                "\n" +
                "{{typography-stub}}</text>\n" +
                "      <sha1>p5s8mgfnepw6exihmw5zf73vjhfh8us</sha1>\n" +
                "    </revision>\n" +
                "  </page>"
        );

        Page page2 = new Page(
                "7697626",
                "Ricky Minard",
                "<page>\n" +
                        "    <title>Ricky Minard</title>\n" +
                        "    <ns>0</ns>\n" +
                        "    <id>7697626</id>\n" +
                        "    <revision>\n" +
                        "      <id>775296214</id>\n" +
                        "      <parentid>764723487</parentid>\n" +
                        "      <timestamp>2017-04-13T23:16:28Z</timestamp>\n" +
                        "      <contributor>\n" +
                        "        <username>Aboutmovies</username>\n" +
                        "        <id>2602832</id>\n" +
                        "      </contributor>\n" +
                        "      <comment>removed [[Category:People from Mansfield, Ohio]]; added [[Category:Sportspeople from Mansfield, Ohio]] using [[WP:HC|HotCat]]</comment>\n" +
                        "      <model>wikitext</model>\n" +
                        "      <format>text/x-wiki</format>\n" +
                        "      <text xml:space=\"preserve\">{{Infobox basketball biography\n" +
                        "| image        =\n" +
                        "| caption      =\n" +
                        "| name         = Ricky Minard\n" +
                        "| position     = [[Shooting guard]] / [[Small forward]]\n" +
                        "| height_ft    = 6\n" +
                        "| height_in    = 5\n" +
                        "| weight_lbs   = 220\n" +
                        "| league       = [[Korisliiga]]\n" +
                        "| team         = Tampereen Pyrintö\n" +
                        "| number       = 24\n" +
                        "| nationality  = American\n" +
                        "| birth_date   = {{birth date and age|1982|9|11}}\n" +
                        "| birth_place  = [[Mansfield, Ohio]]\n" +
                        "| high_school  = [[Mansfield Senior High School|Mansfield]] (Mansfield, Ohio)\n" +
                        "| college      = [[Morehead State Eagles men's basketball|Morehead State]] (2000–2004)\n" +
                        "| draft_year   = 2004\n" +
                        "| draft_round  = 2\n" +
                        "| draft_pick   = 48\n" +
                        "| draft_team   = [[Sacramento Kings]]\n" +
                        "| career_start = 2004\n" +
                        "| career_end   =\n" +
                        "| years1       = 2004–2005\n" +
                        "| team1        = [[Columbus Riverdragons]]\n" +
                        "| years2       = 2005\n" +
                        "| team2        = [[Pallacanestro Biella|Lauretana Biella]]\n" +
                        "| years3       = 2005–2007\n" +
                        "| team3        = [[Pallacanestro Reggiana|Bipop Carire Reggio Emilia]]\n" +
                        "| years4       = 2007–2009\n" +
                        "| team4        = [[Sutor Basket Montegranaro|Premiata Montegranaro]]\n" +
                        "| years5       = 2009–2010\n" +
                        "| team5        = [[Pallacanestro Virtus Roma|Lottomatica Roma]]\n" +
                        "| years6       = 2010\n" +
                        "| team6        = [[BC Khimki|Khimki Moscow Region]]\n" +
                        "| years7       = 2010–2011\n" +
                        "| team7        = [[UNICS Kazan]]\n" +
                        "| years8       = 2011–2012\n" +
                        "| team8        = [[BC Azovmash|Azovmash Mariupol]]\n" +
                        "| years9       = 2012\n" +
                        "| team9        = [[A.S. Junior Pallacanestro Casale|Novipiu Casale]]\n" +
                        "| years10      = 2012–2013\n" +
                        "| team10       = [[Virtus Pallacanestro Bologna|Virtus Bologna]]\n" +
                        "| years11      = 2013\n" +
                        "| team11       = [[Beşiktaş men's basketball team|Beşiktaş]]\n" +
                        "| years12      = 2013–2014\n" +
                        "| team12       = [[BC Budivelnyk|Budivelnyk Kyiv]]\n" +
                        "| years13      = 2015\n" +
                        "| team13       = [[SPO Rouen Basket]]\n" +
                        "| years14      = 2015\n" +
                        "| team14       = [[BG Göttingen]]\n" +
                        "| years15      = 2015–present\n" +
                        "| team15       = [[Tampereen Pyrintö (basketball)|Tampereen Pyrintö]]\n" +
                        "| highlights    =\n" +
                        "* [[Eurocup Basketball|Eurocup]] champion (2011)\n" +
                        "* [[Ukrainian Basketball League|Ukrainian League]] champion (2014)\n" +
                        "* [[Ukrainian Basketball Cup|Ukrainian Cup]] champion (2014)\n" +
                        "}}\n" +
                        "\n" +
                        "'''Ricky Donell Minard Jr.''' (born September 11, 1982) is an American professional [[basketball]] player who plays for [[Tampereen Pyrintö (basketball)|Tampereen Pyrintö]] of the [[Korisliiga]]. He is Morehead State's career scoring leader.&lt;ref name=espn1/&gt;\n" +
                        "\n" +
                        "==Professional career==\n" +
                        "He was selected by the [[Sacramento Kings]] in the 2nd round (48th overall) of the [[2004 NBA Draft]]. A 6'4&quot; guard from [[Morehead State University]], Minard was signed by the Kings in July 2004, but they waived him in November the same year, and so far he has never appeared in an [[NBA]] game.&lt;ref name=espn1&gt;[http://sports.espn.go.com/nba/news/story?id=1836037 Martin signs for three years, $2.34 million - NBA - ESPN]&lt;/ref&gt;\n" +
                        "\n" +
                        "Minard started his professional career with [[Columbus Riverdragons]] of the [[NBDL]].&lt;ref&gt;[http://www.nba.com/dleague/nbdl/draft_041105.html Riverdragons Select Ricky Minard With the No. 1 Overall Pick]&lt;/ref&gt; In January 2005, he left the United States and moved to Italy where he spent next five years playing for [[Pallacanestro Biella|Lauretana Biella]], [[Pallacanestro Reggiana|Bipop Carire Reggio Emilia]], [[Sutor Basket Montegranaro|Premiata Montegranaro]] and [[Pallacanestro Virtus Roma|Lottomatica Roma]].&lt;ref&gt;[http://web.legabasket.it/player/?id=MIN-RIC-82 Italian League profile]&lt;/ref&gt;\n" +
                        "\n" +
                        "On March 12, 2010, he signed with [[BC Khimki|Khimki Moscow Region]] of Russia for the rest of the season.&lt;ref&gt;[http://www.sportando.com/en/europe/russia/80344/khimki-announces-minard.html Khimki announces Minard]&lt;/ref&gt; For the 2010–11 season he stayed in Russia but moved to [[UNICS Kazan]].&lt;ref&gt;[http://www.eurocupbasketball.com/eurocup/news/i/77352/unics-kazan-inks-scorer-minard Unics Kazan inks scorer Minard]&lt;/ref&gt; On July 20, 2011, he signed a one-year deal with [[Azovmash Mariupol]] of Ukraine.&lt;ref&gt;[http://www.sportando.com/en/europe/ukraine/87516/azovmash-mariupol-lands-ricky-minard.html Azovmash Mariupol lands Ricky Minard]&lt;/ref&gt; On February 17, 2012, he was released by Azovmash.&lt;ref&gt;[http://www.sportando.com/en/europe/ukraine/90270/azovmash-mariupol-released-ricky-minard.html Azovmash Mariupol released Ricky Minard]&lt;/ref&gt; The same day he signed with [[A.S. Junior Pallacanestro Casale|Novipiu Casale]] of Italy for the rest of the season.&lt;ref&gt;[http://www.sportando.com/en/italy/serie-a/90279/novipiu-casale-officially-announced-ricky-minard.html Novipiu Casale officially announced Ricky Minard]&lt;/ref&gt;\n" +
                        "\n" +
                        "On August 28, 2012, he signed with [[Virtus Pallacanestro Bologna|Virtus Bologna]] of Italy.&lt;ref&gt;[http://www.sportando.com/en/italy/serie-a/94891/virtus-bologna-officially-signs-ricky-minard.html Virtus Bologna officially signs Ricky Minard]&lt;/ref&gt; On February 21, 2012, he left Bologna and signed with [[Beşiktaş men's basketball team|Beşiktaş]] of Turkey for the rest of the season.&lt;ref&gt;[http://www.sportando.com/en/italy/serie-a/99414/ricky-minard-virtus-bologna-part-ways.html Ricky Minard, Virtus Bologna part ways]&lt;/ref&gt;&lt;ref&gt;[http://www.sportando.com/en/europe/turkey/99416/besiktas-lands-ricky-minard.html Besiktas lands Ricky Minard]&lt;/ref&gt; On July 25, 2013, he signed with [[BC Budivelnyk|Budivelnyk Kyiv]] of Ukraine for the 2013–14 season.&lt;ref&gt;{{cite web|title=Budivelnyk signs Ricky Minard|url=http://www.sportando.net/eng/europe/ukraine/60156/budivelnyk-signs-ricky-minard.html|publisher=Sportando.net|accessdate=July 25, 2013}}&lt;/ref&gt;\n" +
                        "\n" +
                        "On January 18, 2015, he signed with French team [[SPO Rouen Basket]].&lt;ref&gt;[http://www.sportando.com/en/europe/france/149125/rouen-lands-ricky-minard.html Rouen lands Ricky Minard]&lt;/ref&gt; On March 7, 2015, he parted ways with Rouen after appearing in five league games.&lt;ref&gt;[http://www.eurobasket.com/France/news/397227/Ricky-Minard-left-Rouen Ricky Minard left Rouen]&lt;/ref&gt;\n" +
                        "\n" +
                        "On August 20, 2015, he signed with [[BG Göttingen]] of Germany.&lt;ref&gt;[http://www.sportando.com/en/europe/germany/173349/bg-goettingen-lands-ricky-minard.html BG Goettingen lands Ricky Minard]&lt;/ref&gt; On October 23, 2015, he parted ways due to injury with Göttingen after appearing in two games.&lt;ref&gt;[http://www.sportando.com/en/europe/germany/179583/bg-goettingen-ricky-minard-part-ways.html BG Goettingen, Ricky Minard part ways]&lt;/ref&gt; On November 13, 2015, he signed with [[Tampereen Pyrintö (basketball)|Tampereen Pyrintö]] of the Finnish [[Korisliiga]].&lt;ref&gt;[http://www.sportando.com/en/europe/finland/181864/ricky-minard-inks-with-tampere-pyrinto.html Ricky Minard inks with Tampere Pyrinto]&lt;/ref&gt; Pyrintö advanced to Korisliiga Finals, where they lost to [[Kouvot]]. Following their mutual interest already from the previous season the parties re-signed on August 15, 2016.&lt;ref&gt;[http://pyrinto.fi/ricky-minard-pukee-punaista-paalle/ Ricky Minard pukee punaista päälle] Tampereen Pyrintö Basketball {{fi}}&lt;/ref&gt;\n" +
                        "\n" +
                        "==Notes==\n" +
                        "{{Reflist}}\n" +
                        "\n" +
                        "==External links==\n" +
                        "* [http://aol.nba.com/draft2004/profiles/RickyMinard.html NBA.com Draft bio]\n" +
                        "* [http://www.collegehoopsnet.com/Draft/Prospects/RMinard.htm Interview]\n" +
                        "* [http://www.fiba.com/pages/eng/fc/gamecent/p/pid/6026023/playerview.html FIBA.com profile]\n" +
                        "* [http://www.basslinespin.com/MinardRicky.htm Who is Ricky Minard?]\n" +
                        "* [http://basketball.eurobasket.com/player/Ricky_Minard/46336 Eurobasket.com profile]\n" +
                        "\n" +
                        "{{Navboxes|list1=\n" +
                        "{{NBA Development League NumberOne Picks}}\n" +
                        "{{2004 NBA Draft}}\n" +
                        "{{Ohio Valley Conference Men's Basketball Player of the Year navbox}}\n" +
                        "{{BC UNICS 2010–11 Eurocup champions}}\n" +
                        "}}\n" +
                        "\n" +
                        "{{DEFAULTSORT:Minard, Ricky}}\n" +
                        "[[Category:1982 births]]\n" +
                        "[[Category:Living people]]\n" +
                        "[[Category:American expatriate basketball people in Finland]]\n" +
                        "[[Category:American expatriate basketball people in France]]\n" +
                        "[[Category:American expatriate basketball people in Germany]]\n" +
                        "[[Category:American expatriate basketball people in Italy]]\n" +
                        "[[Category:American expatriate basketball people in Russia]]\n" +
                        "[[Category:American expatriate basketball people in Turkey]]\n" +
                        "[[Category:American expatriate basketball people in Ukraine]]\n" +
                        "[[Category:A.S. Junior Pallacanestro Casale players]]\n" +
                        "[[Category:Basketball players from Ohio]]\n" +
                        "[[Category:BC Azovmash players]]\n" +
                        "[[Category:BC Budivelnyk players]]\n" +
                        "[[Category:BC Khimki players]]\n" +
                        "[[Category:BC UNICS players]]\n" +
                        "[[Category:BG Göttingen players]]\n" +
                        "[[Category:Beşiktaş men's basketball players]]\n" +
                        "[[Category:Columbus Riverdragons players]]\n" +
                        "[[Category:Morehead State Eagles men's basketball players]]\n" +
                        "[[Category:Pallacanestro Biella players]]\n" +
                        "[[Category:Pallacanestro Reggiana players]]\n" +
                        "[[Category:Pallacanestro Virtus Roma players]]\n" +
                        "[[Category:Sportspeople from Mansfield, Ohio]]\n" +
                        "[[Category:Sacramento Kings draft picks]]\n" +
                        "[[Category:Shooting guards]]\n" +
                        "[[Category:Small forwards]]\n" +
                        "[[Category:Tampereen Pyrintö players]]\n" +
                        "[[Category:Virtus Pallacanestro Bologna players]]</text>\n" +
                        "      <sha1>hj6cabfwo8k1ec7c0nc7ouoxxgyb0xc</sha1>\n" +
                        "    </revision>\n" +
                        "  </page>"
        );

        Page page3 = new Page(
                "7697757",
                "United Church, The Chapel on the Hill",
                "<page>\n" +
                        "    <title>United Church, The Chapel on the Hill</title>\n" +
                        "    <ns>0</ns>\n" +
                        "    <id>7697757</id>\n" +
                        "    <revision>\n" +
                        "      <id>797121316</id>\n" +
                        "      <parentid>775974506</parentid>\n" +
                        "      <timestamp>2017-08-25T02:29:35Z</timestamp>\n" +
                        "      <contributor>\n" +
                        "        <username>Fortguy</username>\n" +
                        "        <id>3725454</id>\n" +
                        "      </contributor>\n" +
                        "      <comment>Updated infobox</comment>\n" +
                        "      <model>wikitext</model>\n" +
                        "      <format>text/x-wiki</format>\n" +
                        "      <text xml:space=\"preserve\">{{Infobox NRHP\n" +
                        "  | name = United Church, The Chapel on the Hill\n" +
                        "  | partof = Oak Ridge Historic District\n" +
                        "  | nrhp_type = cp | nocat = yes\n" +
                        "  | image = Oak-ridge-united-church-tn1.jpg\n" +
                        "  | caption = The Chapel on the Hill\n" +
                        "  | location= [[Oak Ridge, Tennessee]]\n" +
                        "  | coordinates = {{coord|36.03046|-84.24226|display=inline,title}}\n" +
                        "  | locmapin = Tennessee#USA\n" +
                        "  | built = 1943\n" +
                        "  | architect = [[U.S. Army Corps of Engineers]]\n" +
                        "  | architecture =\n" +
                        "  | added = September 05, 1991\n" +
                        "  | area = 700-series [[U.S. Army]] chapel\n" +
                        "  | mpsub = {{NRHP url|id=64500613|title=Oak Ridge MPS}}\n" +
                        "  | refnum = 91001109&lt;ref name=&quot;nris&quot;&gt;{{NRISref|2010a}}&lt;/ref&gt;\n" +
                        "}}\n" +
                        "The '''United Church, Chapel on the Hill''' in [[Oak Ridge, Tennessee]] was the city's main church during [[World War II]]. Dedicated on September 30, 1943 and completed late in October 1943, it was originally a multi-denominational [[chapel]] shared by [[Catholic Church|Catholic]], [[Protestantism|Protestant]] and [[Judaism|Jewish]] congregations.&lt;ref name=NationalRegister/&gt;&lt;ref name=churchhistory&gt;[http://thechapelonthehill.org/index.php?option=com_content&amp;task=category&amp;sectionid=4&amp;id=39&amp;Itemid=55 Church History] {{webarchive |url=https://web.archive.org/web/20110728092956/http://thechapelonthehill.org/index.php?option=com_content&amp;task=category&amp;sectionid=4&amp;id=39&amp;Itemid=55 |date=July 28, 2011 }}, United Church, Chapel on the Hill website, accessed March 5, 2010&lt;/ref&gt;\n" +
                        "\n" +
                        "== Architectural design ==\n" +
                        "The building design is a [[U.S. Army Corps of Engineers]] 700-series [[U.S. Army]] chapel.&lt;ref name=Townsite&gt;[http://oakridgevisitor.com/history/pdf/OriginalTownSites.pdf Chapel on the Hill] {{webarchive |url=https://web.archive.org/web/20080517124718/http://oakridgevisitor.com/history/pdf/OriginalTownSites.pdf |date=May 17, 2008 }} in ''Jackson Square – the original Townsite'', Oak Ridge Convention and Visitors' Bureau&lt;/ref&gt; It is a frame building built on a three-bay rectangular plan with a [[Steeple (architecture)|steeple]]d [[bell tower]] and a [[gable]] entry [[porch]]. It was one of three Army chapels built using the same design in Oak Ridge during World War II. One of the other two chapels, the West Chapel in the city's West Village area, was later torn down, but the East Chapel in the East Village is still in use.&lt;ref name=NationalRegister&gt;{{cite web|url=http://www.cfo.doe.gov/me70/history/NPSweb/Oak_Ridge_Historic_District_(Preferred).pdf |title=National Register of Historic Places Registration Form for Oak Ridge Historic District |date=July 18, 1991 |archiveurl=https://web.archive.org/web/20100829041710/http://www.cfo.doe.gov/me70/history/NPSweb/Oak_Ridge_Historic_District_%28Preferred%29.pdf |archivedate=August 29, 2010}}&lt;/ref&gt;\n" +
                        "\n" +
                        "== History ==\n" +
                        "The U.S. Army built the chapel to house religious activities, as one of numerous community facilities in the &quot;townsite&quot; area of Oak Ridge. The building was dedicated on September 30, 1943, in a ceremony that included [[prayer]]s and talks by a Jewish [[rabbi]], a [[priesthood (Catholic Church)|Catholic priest]], an [[Episcopal Church (United States)|Episcopal]] priest, a [[Baptist]] minister, and the [[minister (Christianity)|minister]] who was serving the United Church congregation that eventually came to own the chapel.&lt;ref&gt;Charles W. Johnson and Charles O. Jackson (1981), ''City Behind a Fence: Oak Ridge, Tennessee, 1942-1946''. The University of Tennessee Press. Pages 127-129.&lt;/ref&gt; Its name, &quot;The Chapel on the Hill,&quot; comes from a reference in a prayer by the Knoxville Baptist minister who participated in the dedication.&lt;ref name=Townsite/&gt;\n" +
                        "\n" +
                        "The United Church congregation that is housed in the Chapel on the Hill traces its history to July 18, 1943, when some 25 to 30 Christians of diverse denominational backgrounds gathered for Sunday worship in Oak Ridge's main [[cafeteria]]. Subsequently, several members of the group made plans to establish an interdenominational Protestant church, led by [[laity|layperson]]s, to include all denominations. A [[Presbyterian]] minister working in [[Knoxville, Tennessee|Knoxville]] was engaged to conduct weekly services, and about 150 people representing 13 Protestant denominations became charter members of &quot;the United Church&quot;. Governing boards of laypersons elected to lead the new congregation took up their duties on October 24, 1943.&lt;ref name=pre1980&gt;[http://thechapelonthehill.org/index.php?option=com_content&amp;task=view&amp;id=51&amp;Itemid=55 Pre-1980 Historical Compilation] {{webarchive |url=https://web.archive.org/web/20110728093006/http://thechapelonthehill.org/index.php?option=com_content&amp;task=view&amp;id=51&amp;Itemid=55 |date=July 28, 2011 }}, United Church, Chapel on the Hill website, accessed March 5, 2010&lt;/ref&gt;\n" +
                        "\n" +
                        "When the Chapel-on-the-Hill was completed that same month, the United Church and the local Roman Catholic Church were given control of the building, as the only two churches then officially operating in the [[Manhattan Project]] community. During the war, when Oak Ridge's Manhattan Project facilities were operating around the clock, the chapel building was also in use nearly 24 hours a day as a venue for worship services, [[wedding]]s, and other occasions for local workers of various Protestant, Catholic, and Jewish religious backgrounds.&lt;ref name=pre1980/&gt;\n" +
                        "\n" +
                        "At the peak of wartime activity in Oak Ridge, when the population exceeded 70,000, the United Church employed four ministers and conducted worship services in the Chapel on the Hill, East Village Chapel, and the Jefferson Theater, as well as [[Sunday school]] classes in several local schools and a [[mobile home|trailer]] camp. By 1951, the United Church Chapel-on-the-Hill consolidated as a single [[interdenominational]] congregation, making its home in the Chapel on the Hill building.&lt;ref name=pre1980/&gt;\n" +
                        "\n" +
                        "The United Church congregation purchased the chapel and {{convert|3.72|acre|ha}} of land from the [[U.S. Atomic Energy Commission]] on May 11, 1955 for a price of $17,116.&lt;ref name=pre1980/&gt; An adjoining educational building was added in 1956-1957. The facility continues to operate as a nondenominational Protestant church under lay leadership, employing ministers with backgrounds in [[Mainline (Protestant)|mainstream Protestant]] denominations.&lt;ref&gt;Don Goodwin, [http://thechapelonthehill.org/index.php?option=com_content&amp;task=view&amp;id=52&amp;Itemid=55 &quot;The United Church&quot; (1980) and &quot;A Unique and Lonely Path&quot; (2007)] {{webarchive |url=https://web.archive.org/web/20110728093011/http://thechapelonthehill.org/index.php?option=com_content&amp;task=view&amp;id=52&amp;Itemid=55 |date=July 28, 2011 }}, United Church, Chapel on the Hill website, accessed March 5, 2010&lt;/ref&gt; Since 2007 it has been affiliated with the [[Center for Progressive Christianity]].&lt;ref&gt;[http://thechapelonthehill.org/index.php?option=com_content&amp;task=view&amp;id=175&amp;Itemid=49 The 8 Points of Progressive Christianity] {{webarchive |url=https://web.archive.org/web/20120304033528/http://thechapelonthehill.org/index.php?option=com_content&amp;task=view&amp;id=175&amp;Itemid=49 |date=March 4, 2012 }}, United Church, Chapel on the Hill website, accessed March 5, 2010&lt;/ref&gt; The church's motto is &quot;Where People from All Denominations Meet in Their Differences, but Are One in Their Search for God.&quot;&lt;ref name=churchhistory/&gt;\n" +
                        "\n" +
                        "The Chapel-on-the-Hill was placed on the [[National Register of Historic Places]] in 1993 as a [[contributing property]] in the [[Oak Ridge Historic District]].&lt;ref name=NationalRegister/&gt;\n" +
                        "\n" +
                        "==References==\n" +
                        "{{reflist}}\n" +
                        "\n" +
                        "==External links==\n" +
                        "* [http://www.thechapelonthehill.org The United Church, Chapel on the Hill]\n" +
                        "\n" +
                        "{{DEFAULTSORT:United Church, The Chapel On The Hill}}\n" +
                        "[[Category:Churches in Tennessee]]\n" +
                        "[[Category:Churches on the National Register of Historic Places in Tennessee]]\n" +
                        "[[Category:Oak Ridge, Tennessee]]\n" +
                        "[[Category:Buildings and structures in Anderson County, Tennessee]]\n" +
                        "[[Category:Military chapels of the United States]]\n" +
                        "[[Category:Nondenominational Christian churches in Tennessee]]\n" +
                        "[[Category:Wooden churches in Tennessee]]\n" +
                        "[[Category:Historic district contributing properties in Tennessee]]\n" +
                        "[[Category:National Register of Historic Places in Anderson County, Tennessee]]</text>\n" +
                        "      <sha1>put30c9dfybsrh5h2xf0wtyfqpgkagj</sha1>\n" +
                        "    </revision>\n" +
                        "  </page>"
        );


        SparkConf conf = new SparkConf().setAppName("SparkWikistats");

        JavaSparkContext spark = new JavaSparkContext(conf);

        JavaRDD<Page> pages = spark.parallelize(new ArrayList<Page>() {{
            add(page1);
            add(page2);
            add(page3);
        }});

        JavaRDD<Integer> links_per_pages = pages.map(page -> (StringUtils.countMatches(page.getPageContent(), "* [http:")));

        JavaRDD<Integer> categories_per_pages = pages.map(page -> (StringUtils.countMatches(page.getPageContent(), "[[Category")));

        System.out.println("###########LINKS PER PAGE: " + links_per_pages.collect() + "###########");

        System.out.println("###########LINKS PER PAGE: " + categories_per_pages.collect() + "###########");

        spark.stop();
    }

}
