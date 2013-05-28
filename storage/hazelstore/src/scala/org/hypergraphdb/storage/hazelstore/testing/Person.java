package org.hypergraphdb.storage.hazelstore.testing;


public class Person {

    public Person() {}   // null constructor

    public Person(int id,String name, String adress, String email, Integer birthyear) {
        this.ID = id;
        this.name = name;
        this.adress = adress;
        this.email = email;
        this.birthyear = birthyear;
    }

    int ID;
    public int getID() {return ID;}
    public void setID(int ID) {this.ID = ID;}

    String name;
    public String getName() {return name;   }
    public void setName(String name) {this.name = name;  }

    String adress;
    public String getAdress() {return adress; }
    public void setAdress(String adress) {this.adress = adress;     }

    String email;
    public String getEmail() {return email;    }
    public void setEmail(String email) {this.email = email;    }

    Integer birthyear;
    public Integer getBirthyear() {return birthyear;}
    public void setBirthyear(Integer birthyear) {this.birthyear = birthyear;}

    public String toString(){
        return "ID" + ID + ". Name:" + getName() + ". Adress: " + getAdress() +". email: " + getEmail() + ". insuranceID" + getBirthyear();
    }
}