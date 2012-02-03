/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hgtest.beans;

import java.io.Serializable;

public class Folder implements Serializable {

    private String name;

    public Folder() {
    }

    public Folder(String name) {
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        if (this.name == null) {
            return "No name";
        }
        if (this.name.length() == 0) {
            return "";
        }
        return name;
    }
}
