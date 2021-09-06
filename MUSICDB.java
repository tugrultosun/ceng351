/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ceng.ceng351.musicdb;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import ceng.ceng351.musicdb.QueryResult.ArtistNameSongNameGenreRatingResult;
import ceng.ceng351.musicdb.QueryResult.TitleReleaseDateRatingResult;
import ceng.ceng351.musicdb.QueryResult.ArtistNameNumberOfSongsResult;
import ceng.ceng351.musicdb.QueryResult.UserIdUserNameNumberOfSongsResult;
/**
 *
 * 
 */


public class MUSICDB implements IMUSICDB{

   
    private static String uname="e2099406",pass="",dbname="db2099406";
    private static String dbhost="";
    private static int port=8084;
    private Connection con;
    
    public MUSICDB(){}
    
    @Override
    public void initialize() {
        String url = "jdbc:mysql://"+this.dbhost+":"+this.port+"/"+this.dbname;
		try
		{
                        Class.forName("com.mysql.cj.jdbc.Driver");
			this.con = DriverManager.getConnection(url, this.uname, this.pass);
		}
		catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } 
    }           
    @Override
    public int createTables() {
        int result;
        int numberofTablesInserted = 0;


        
        
        //user(userID:int, userName:varchar(60), email:varchar(30), password:varchar(30))
        String queryCreateUserTable = "create table user (" + 
                                                                   "userID int not null," + 
                                                                   "userName varchar(60)," + 
                                                                   "email varchar(30)," +
                                                                   "password varchar(30)," +                                
                                                                   "primary key (userID));";
        try {
            Statement statement = this.con.createStatement();

            //User Table
            result = statement.executeUpdate(queryCreateUserTable);
            System.out.println(result);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        //artist(artistID:int, artistName:varchar(60))
        String queryCreateartistTable = "create table artist (" + 
                                                                   "artistID int not null," + 
                                                                   "artistName varchar(60)," +                                                                                                
                                                                   "primary key (artistID));";
        try {
            Statement statement = this.con.createStatement();

            //artist Table
            result = statement.executeUpdate(queryCreateartistTable);
            System.out.println(result);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        //album(albumID:int, title:varchar(60), albumGenre:varchar(30), albumRating:double, releaseDate:date, artistID:int)
        String queryCreatealbumTable = "create table album (" + 
                                                                   "albumID int not null," + 
                                                                   "title varchar(60)," +
                                                                   "albumGenre varchar(30)," +
                                                                   "albumRating double," +
                                                                   "releaseDate date," +
                                                                   "artistID int," + 
                                                                   "primary key (albumID),"+
                                                                   "foreign key(artistID) references artist(artistID) on delete cascade);";
        try {
            Statement statement = this.con.createStatement();

            //album Table
            result = statement.executeUpdate(queryCreatealbumTable);
            System.out.println(result);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        String queryCreatesongTable = "create table song (" + 
                                                                "songID int not null," + 
                                                                "songName varchar(60)," + 
                                                                "genre varchar(30)," +
                                                                "rating double," + 
                                                                "artistID int," +
                                                                "albumID int," +
                                                                "primary key (songID),"+
                                                                "foreign key(artistID) references artist(artistID) on delete cascade,"+
                                                                "foreign key (albumID) references album(albumID) on delete cascade);";
        try {
            Statement statement = this.con.createStatement();

            //song Table
            result = statement.executeUpdate(queryCreatesongTable);
            System.out.println(result);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        
        //listen(userID:int, songID:int, lastListenTime:timestamp, listenCount:int)
        String queryCreatelistenTable = "create table listen (" + 
                                                                   "userID int not null," + 
                                                                   "songID int not null," +
                                                                   "lastListenTime timestamp," +
                                                                   "listenCount int,"+
                                                                   "primary key(userID,songID),"+
                                                                   "foreign key (userID) references user(userID) on delete cascade,"+
                                                                   "foreign key (songID) references song(songID) on delete cascade);";
        try {
            Statement statement = this.con.createStatement();

            //album Table
            result = statement.executeUpdate(queryCreatelistenTable);
            System.out.println(result);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
          

        return numberofTablesInserted;
    }

    

    @Override
    public int dropTables() {
        int result;
        int numberofTablesDropped = 0;
        
        String queryDroplistenTable = "drop table if exists listen";


		try {
			Statement statement = this.con.createStatement();


			result = statement.executeUpdate(queryDroplistenTable);
			numberofTablesDropped++;
			System.out.println(result);


			//close
			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			// TODO
		}
        
        String queryDropsongTable = "drop table if exists song";


		try {
			Statement statement = this.con.createStatement();


			result = statement.executeUpdate(queryDropsongTable);
			numberofTablesDropped++;
			System.out.println(result);


			//close
			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			// TODO
		}        
        
        String queryDropalbumTable = "drop table if exists album";


		try {
			Statement statement = this.con.createStatement();


			result = statement.executeUpdate(queryDropalbumTable);
			numberofTablesDropped++;
			System.out.println(result);


			//close
			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			// TODO
		}        
        
        String queryDropartistTable = "drop table if exists artist";


		try {
			Statement statement = this.con.createStatement();


			result = statement.executeUpdate(queryDropartistTable);
			numberofTablesDropped++;
			System.out.println(result);


			//close
			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			// TODO
		}        
                
                
        String queryDropuserTable = "drop table if exists user";


		try {
			Statement statement = this.con.createStatement();


			result = statement.executeUpdate(queryDropuserTable);
			numberofTablesDropped++;
			System.out.println(result);


			//close
			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			// TODO
		}    
        return numberofTablesDropped;
    }

    @Override
    public int insertAlbum(Album[] albums) {
        int numberofInsertAlbum = 0;
	int result;
	for (int i = 0; i < albums.length; i++) {
		String query = "insert into album values (\"" +
				albums[i].getAlbumID() + "\",\"" +
				albums[i].getTitle() + "\",\"" +
				albums[i].getAlbumGenre() + "\",\"" +
                                albums[i].getAlbumRating() + "\",\"" +
                                albums[i].getReleaseDate() + "\",\"" +
				albums[i].getArtistID() + "\")";
			try {
				Statement st = this.con.createStatement();
				result = st.executeUpdate(query);
				System.out.println(result);
				numberofInsertAlbum++;
				//Close
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();

			}
		}

		return numberofInsertAlbum;
    }

    @Override
    public int insertArtist(Artist[] artists) {
        int numberofInsertArtist = 0;
	int result;
	for (int i = 0; i < artists.length; i++) {
		String query = "insert into artist values (\"" +
				artists[i].getArtistID() + "\",\"" +
				artists[i].getArtistName() + "\")";
			try {
				Statement st = this.con.createStatement();
				result = st.executeUpdate(query);
				System.out.println(result);
				numberofInsertArtist++;
				//Close
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();

			}
		}

		return numberofInsertArtist;
    }

    @Override
    public int insertSong(Song[] songs) {
        int numberofInsertSong = 0;
	int result;
	for (int i = 0; i < songs.length; i++) {
		String query = "insert into song values (\"" +
				songs[i].getSongID() + "\",\"" +
				songs[i].getSongName() + "\",\"" +
				songs[i].getGenre() + "\",\"" +
                                songs[i].getRating() + "\",\"" +
                                songs[i].getArtistID() + "\",\"" +
				songs[i].getAlbumID() + "\")";
			try {
				Statement st = this.con.createStatement();
				result = st.executeUpdate(query);
				System.out.println(result);
				numberofInsertSong++;
				//Close
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();

			}
		}

		return numberofInsertSong;
    }

    @Override
    public int insertUser(User[] users) {
        int numberofInsertUser = 0;
	int result;
	for (int i = 0; i < users.length; i++) {
		String query = "insert into user values (\"" +
				users[i].getUserID() + "\",\"" +
				users[i].getUserName() + "\",\"" +
				users[i].getEmail() + "\",\"" +
				users[i].getPassword() + "\")";
			try {
				Statement st = this.con.createStatement();
				result = st.executeUpdate(query);
				System.out.println(result);
				numberofInsertUser++;
				//Close
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();

			}
		}

		return numberofInsertUser;
    }

    @Override
    public int insertListen(Listen[] listens) {
        int numberofInsertListen = 0;
	int result;
	for (int i = 0; i < listens.length; i++) {
		String query = "insert into listen values (\"" +
				listens[i].getUserID() + "\",\"" +
				listens[i].getSongID()+ "\",\"" +
				listens[i].getLastListenTime() + "\",\"" +
                                listens[i].getListenCount() + "\")";           
			try {
				Statement st = this.con.createStatement();
				result = st.executeUpdate(query);
				System.out.println(result);
				numberofInsertListen++;
				//Close
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();

			}
		}

		return numberofInsertListen;
    }

    @Override
    public QueryResult.ArtistNameSongNameGenreRatingResult[] getHighestRatedSongs() {
        ResultSet rs;

		List<ArtistNameSongNameGenreRatingResult>  nsngrlist = new ArrayList<ArtistNameSongNameGenreRatingResult>();

		String query = "select a.artistName, s.songName, s.genre,s.rating from artist a inner join song s on s.artistID=a.artistID "+
		"where s.rating>= (select max(s1.rating) from song s1) order by a.artistName asc";

		try {

			Statement st  = this.con.createStatement();
			rs = st.executeQuery(query);

			while(rs.next()){
				ArtistNameSongNameGenreRatingResult nsngr = null;

				String artistName = rs.getString("artistName");
				String songName = rs.getString("songName");
				String genre= rs.getString("genre");
                                double rating=rs.getDouble("rating");

				nsngr = new ArtistNameSongNameGenreRatingResult(artistName,songName,genre,rating);
				nsngrlist.add(nsngr);
			}

		}catch (SQLException e) {
			e.printStackTrace();

		}
		ArtistNameSongNameGenreRatingResult[] nsngrArray = new ArtistNameSongNameGenreRatingResult[nsngrlist.size()];
		nsngrArray = nsngrlist.toArray(nsngrArray);
		return nsngrArray;
    }

    @Override
    public QueryResult.TitleReleaseDateRatingResult getMostRecentAlbum(String artistName) {
        ResultSet rs;

		TitleReleaseDateRatingResult trdrr = null;

		String query = "select a.title, a.releaseDate, a.albumRating from album a inner join artist b on a.artistID=b.artistID "+
                        " where b.artistName=\""+artistName+"\"and a.releaseDate=(select max(a1.releaseDate) "+
                        " from album a1,artist b1 " +
                        " where a1.artistID=b1.artistID and b1.artistName=\""+artistName+"\")";

		try {

			Statement st  = this.con.createStatement();
			rs = st.executeQuery(query);
                        while(rs.next()){
                            String title = rs.getString("title");
                            String releaseDate= rs.getString("releaseDate");
                            double albumRating=rs.getDouble("albumRating");

                            trdrr = new TitleReleaseDateRatingResult(title,releaseDate,albumRating);
                        }

		}catch (SQLException e) {
			e.printStackTrace();

		}
		return trdrr;
    }

    @Override
    public QueryResult.ArtistNameSongNameGenreRatingResult[] getCommonSongs(String userName1, String userName2) {
        ResultSet rs;

		List<ArtistNameSongNameGenreRatingResult>  nsngrlist = new ArrayList<ArtistNameSongNameGenreRatingResult>();

		String query = "select a.artistName, s.songName, s.genre,s.rating from user u1,user u2,listen l1,listen l2,artist a inner join song s on s.artistID=a.artistID "+
		"where u1.userName=\""+userName1+"\" and u2.userName=\""+userName2+"\" and l1.userID=u1.userID and l2.userID=u2.userID and l1.songID=l2.songID and l1.songID=s.songID "+
                " order by s.rating desc";

		try {

			Statement st  = this.con.createStatement();
			rs = st.executeQuery(query);

			while(rs.next()){
				ArtistNameSongNameGenreRatingResult nsngr = null;

				String artistName = rs.getString("artistName");
				String songName = rs.getString("songName");
				String genre= rs.getString("genre");
                                double rating=rs.getDouble("rating");

				nsngr = new ArtistNameSongNameGenreRatingResult(artistName,songName,genre,rating);
				nsngrlist.add(nsngr);
			}

		}catch (SQLException e) {
			e.printStackTrace();

		}
		ArtistNameSongNameGenreRatingResult[] nsngrArray = new ArtistNameSongNameGenreRatingResult[nsngrlist.size()];
		nsngrArray = nsngrlist.toArray(nsngrArray);
		return nsngrArray;
    }

    @Override
    public QueryResult.ArtistNameNumberOfSongsResult[] getNumberOfTimesSongsListenedByUser(String userName) {
        ResultSet rs;

		List<ArtistNameNumberOfSongsResult>  annosrlist = new ArrayList<ArtistNameNumberOfSongsResult>();

		String query = "select a.artistName,sum(l.listenCount) from user u,artist a,listen l, song s "+
		"where s.songID=l.songID and l.userID=u.userID and u.userName=\""+userName+"\" and s.artistID=a.artistID group by a.artistName order by a.artistName";

		try {

			Statement st  = this.con.createStatement();
			rs = st.executeQuery(query);

			while(rs.next()){
				ArtistNameNumberOfSongsResult annosr = null;

				String artistName = rs.getString("artistName");
				int listenCount = rs.getInt("sum(l.listenCount)");
                                
				annosr = new ArtistNameNumberOfSongsResult(artistName,listenCount);
				annosrlist.add(annosr);
			}

		}catch (SQLException e) {
			e.printStackTrace();

		}
		ArtistNameNumberOfSongsResult[] annosrArray = new ArtistNameNumberOfSongsResult[annosrlist.size()];
		annosrArray = annosrlist.toArray(annosrArray);
		return annosrArray;
    }

    @Override
    public User[] getUsersWhoListenedAllSongs(String artistName) {
            ResultSet rs;

		List<User>  userlist = new ArrayList<User>();

		String query = "select u.userID,u.userName,u.email,u.password from user u where not exists (select s.songID "+
		"from song s,artist a where a.artistName=\""+artistName+"\" and s.artistID=a.artistID and not exists "+
                "(select l.songID from listen l where l.songID=s.songID and l.userID=u.userID order by u.userID))";
               

		try {

			Statement st  = this.con.createStatement();
			rs = st.executeQuery(query);

			while(rs.next()){
				User user_v = null;

				int userID = rs.getInt("userID");
				String userName = rs.getString("userName");
                                String email = rs.getString("email");
                                String password = rs.getString("password");
				user_v = new User(userID,userName,email,password);
				userlist.add(user_v);
			}

		}catch (SQLException e) {
			e.printStackTrace();

		}
		User[] userArray = new User[userlist.size()];
		userArray = userlist.toArray(userArray);
		return userArray;
    }

    @Override
    public QueryResult.UserIdUserNameNumberOfSongsResult[] getUserIDUserNameNumberOfSongsNotListenedByAnyone() {
        ResultSet rs;

		List<UserIdUserNameNumberOfSongsResult>  uı_un_noslist = new ArrayList<UserIdUserNameNumberOfSongsResult>();

		String query = "select distinct u1.userID,u1.userName,count(distinct l1.songID) as songcount from user u1,user u2,listen l1 " +
                                "where l1.userID=u1.userID and u1.userID<>u2.userID and "+
                        "l1.songID not in(select l2.songID from listen l2 where l2.userID<>u1.userID) group by u1.userID;";

		try {

			Statement st  = this.con.createStatement();
			rs = st.executeQuery(query);

			while(rs.next()){
				UserIdUserNameNumberOfSongsResult uı_un_nos = null;

				int userID = rs.getInt("userID");
				String userName = rs.getString("userName");
                                int songcount=rs.getInt("songcount");
				uı_un_nos = new UserIdUserNameNumberOfSongsResult(userID,userName,songcount);
				uı_un_noslist.add(uı_un_nos);
			}

		}catch (SQLException e) {
			e.printStackTrace();

		}
		UserIdUserNameNumberOfSongsResult[] uı_un_nosArray = new UserIdUserNameNumberOfSongsResult[uı_un_noslist.size()];
		uı_un_nosArray = uı_un_noslist.toArray(uı_un_nosArray);
		return uı_un_nosArray;
    }

    @Override
    public Artist[] getArtistSingingPopGreaterAverageRating(double rating) {
        ResultSet rs;

		List<Artist>  artistlist = new ArrayList<Artist>();

		String query = "select distinct a.artistID,a.artistName from artist a,song s,song s2 where s.artistID=a.artistID and "+
                                "s.genre=\"Pop\" and s2.artistID=a.artistID group by a.artistID having avg(s2.rating)>\""+rating+"\"";

		try {

			Statement st  = this.con.createStatement();
			rs = st.executeQuery(query);
                            
			while(rs.next()){
				Artist myartist = null;

				int artistID = rs.getInt("artistID");
                                String artistname = rs.getString("artistName");
                              

				myartist= new Artist(artistID,artistname);
				artistlist.add(myartist);
			}

		}catch (SQLException e) {
			e.printStackTrace();

		}
		Artist[] myArray = new Artist[artistlist.size()];
		myArray = artistlist.toArray(myArray);
		return myArray;
    }

    @Override
    public Song[] retrieveLowestRatedAndLeastNumberOfListenedSongs() {
        ResultSet rs;

		List<Song>  songlist = new ArrayList<Song>();

		String query = "select s.songID,s.songName,s.genre,s.rating,s.artistID,s.albumID,sum(l.listenCount) as listencount "+
                                "from song s,listen l where s.genre=\"Pop\" and "+
                                "s.rating=(select min(s2.rating) from song s2 where s2.genre=\"Pop\") and l.songID=s.songID group by s.songID having listencount=(min(listencount))";

		try {

			Statement st  = this.con.createStatement();
			rs = st.executeQuery(query);
                            
			while(rs.next()){
				Song least_listened_pop_music = null;

				int songID = rs.getInt("songID");
                                String sname = rs.getString("songName");
                                String genre =rs.getString("genre");
                                double rating=rs.getDouble("rating");
                                int artistID = rs.getInt("artistID");
                                int albumID=rs.getInt("albumID");

				least_listened_pop_music = new Song(songID, sname, genre,rating,artistID,albumID);
				songlist.add(least_listened_pop_music);
			}

		}catch (SQLException e) {
			e.printStackTrace();

		}
		Song[] leastlistenedpopsongsArray = new Song[songlist.size()];
		leastlistenedpopsongsArray = songlist.toArray(leastlistenedpopsongsArray);
		return leastlistenedpopsongsArray;
    }

    @Override
    public int multiplyRatingOfAlbum(String releaseDate) {
        int result = 0;;


		String query ="update album a set a.albumRating = a.albumRating*1.5 where a.releaseDate > \""+releaseDate+ "\"";
		System.out.println(query);
		try {

			Statement st  = this.con.createStatement();
			result = st.executeUpdate(query);
		}catch (SQLException e) {
			e.printStackTrace();

		}
		return result;
    }

    @Override
    public Song deleteSong(String songName) {
        ResultSet rs;

		Song song_to_be_deleted=null;

		String deleting_query = "delete from song where songName=\""+songName+"\"";
		System.out.println(deleting_query);
		try {
			Statement st  = this.con.createStatement();
                        
                        String query = "select s.songID,s.songName, s.genre,s.rating,s.artistID,s.albumID from song s where s.songName=\""+songName+"\"";
                        //first create object that will be deleted
			rs = st.executeQuery(query);
                        rs.next();
                       // st.executeUpdate(deleting_query);
			int songID = rs.getInt("songID");
			String sname = rs.getString("songName");
                        String genre =rs.getString("genre");
                        double rating=rs.getDouble("rating");
			int artistID = rs.getInt("artistID");
                        int albumID=rs.getInt("albumID");
                        song_to_be_deleted = new  Song(songID, sname, genre,rating,artistID,albumID);
                        st.executeUpdate(deleting_query);
                        
                        
		}catch (SQLException e){
			e.printStackTrace();

		}
		return song_to_be_deleted;
    }
    
} 

    