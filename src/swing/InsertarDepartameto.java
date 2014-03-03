/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package swing;

/**
 *
 * @author alumno
 */
public class InsertarDepartameto extends javax.swing.JFrame {

    private AccesoJDBC jdbc;
    
    /**
     * Creates new form InsertarDepartameto
     */
    public InsertarDepartameto() {
        initComponents();
        jdbc = new AccesoJDBC();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        label_dept_no = new javax.swing.JLabel();
        d_dept_no = new javax.swing.JTextField();
        label_dnombre = new javax.swing.JLabel();
        d_dnombre = new javax.swing.JTextField();
        d_loc = new javax.swing.JTextField();
        label_loc = new javax.swing.JLabel();
        jButton1 = new javax.swing.JButton();
        label_resultado = new javax.swing.JLabel();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        label_dept_no.setHorizontalAlignment(javax.swing.SwingConstants.RIGHT);
        label_dept_no.setText("Numero de Departamento:");

        label_dnombre.setHorizontalAlignment(javax.swing.SwingConstants.RIGHT);
        label_dnombre.setText("Nombre de Departamento:");

        d_dnombre.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                d_dnombreActionPerformed(evt);
            }
        });

        d_loc.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                d_locActionPerformed(evt);
            }
        });

        label_loc.setHorizontalAlignment(javax.swing.SwingConstants.RIGHT);
        label_loc.setText("Localización:");

        jButton1.setText("Grabar");
        jButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });

        label_resultado.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        label_resultado.setText("-");

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(label_resultado, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 90, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(javax.swing.GroupLayout.Alignment.LEADING, layout.createSequentialGroup()
                        .addComponent(label_dept_no, javax.swing.GroupLayout.PREFERRED_SIZE, 144, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(d_dept_no, javax.swing.GroupLayout.PREFERRED_SIZE, 110, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(javax.swing.GroupLayout.Alignment.LEADING, layout.createSequentialGroup()
                        .addComponent(label_dnombre, javax.swing.GroupLayout.PREFERRED_SIZE, 144, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(d_dnombre))
                    .addGroup(javax.swing.GroupLayout.Alignment.LEADING, layout.createSequentialGroup()
                        .addComponent(label_loc, javax.swing.GroupLayout.PREFERRED_SIZE, 144, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(d_loc)))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(label_dept_no, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(d_dept_no, javax.swing.GroupLayout.DEFAULT_SIZE, 30, Short.MAX_VALUE))
                .addGap(17, 17, 17)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(label_dnombre, javax.swing.GroupLayout.DEFAULT_SIZE, 30, Short.MAX_VALUE)
                    .addComponent(d_dnombre))
                .addGap(18, 18, 18)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(label_loc, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(d_loc, javax.swing.GroupLayout.PREFERRED_SIZE, 30, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(18, 18, 18)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 35, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(label_resultado, javax.swing.GroupLayout.PREFERRED_SIZE, 35, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void d_dnombreActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_d_dnombreActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_d_dnombreActionPerformed

    private void d_locActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_d_locActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_d_locActionPerformed

    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
        int res = jdbc.insDept(Integer.valueOf(this.d_dept_no.getText()),
                                this.d_dnombre.getText().toUpperCase(),
                                this.d_loc.getText().toUpperCase());
        this.label_resultado.setText((res==0)?"Fallo en la inserción":"Alta Realizada");
    }//GEN-LAST:event_jButton1ActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(InsertarDepartameto.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(InsertarDepartameto.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(InsertarDepartameto.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(InsertarDepartameto.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new InsertarDepartameto().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JTextField d_dept_no;
    private javax.swing.JTextField d_dnombre;
    private javax.swing.JTextField d_loc;
    private javax.swing.JButton jButton1;
    private javax.swing.JLabel label_dept_no;
    private javax.swing.JLabel label_dnombre;
    private javax.swing.JLabel label_loc;
    private javax.swing.JLabel label_resultado;
    // End of variables declaration//GEN-END:variables
}
