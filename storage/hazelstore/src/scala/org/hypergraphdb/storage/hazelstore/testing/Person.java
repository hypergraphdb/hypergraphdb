package org.hypergraphdb.storage.hazelstore.testing;


public class Person {

    public Person() {}   // null constructor

    public Person(String name, String adress, String email, Integer insurance) {
        this.name = name;
        this.adress = adress;
        this.email = email;
        this.insurance = insurance;
    }

    String name;
    public String getName() {return name;   }
    public void setName(String name) {this.name = name;  }

    String adress;
    public String getAdress() {return adress; }
    public void setAdress(String adress) {this.adress = adress;     }

    String email;
    public String getEmail() {return email;    }
    public void setEmail(String email) {this.email = email;    }

    Integer insurance;
    public Integer getInsurance() {return insurance;    }
    public void setInsurance(Integer insurance) {this.insurance = insurance;}

    public String toString(){
        return "Name:" + getName() + ". Adress: " + getAdress() +". email: " + getEmail() + ". insuranceID" + getInsurance();
    }
}